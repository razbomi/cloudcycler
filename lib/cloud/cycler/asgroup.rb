require 'json'
require 'cloud/cycler/namespace'

# Wrapper around AWS::AutoScaling.
class Cloud::Cycler::ASGroup
  attr_accessor :grace_period

  def initialize(task, name)
    @task         = task
    @name         = name
    @grace_period = 30
  end

  # Restart any stopped instances, and resume autoscaling processes.
  def start
    if !autoscaling_group.exists?
      @task.warn { "Autoscaling group #{@name} doesn't exist" }
      return
    end
    
    suspended = load_from_s3(@task.bucket)

    # FIXME: This won't work if we reinstate suspended processes...
    #if autoscaling_group.suspended_processes.empty?
      #@task.debug { "Scaling group #{@name} already running" }
    #else
      start_instances

      @task.unsafe("Resuming #{@name} processes") do
        autoscaling_group.resume_all_processes
        autoscaling_group.suspend_processes suspended.keys
      end
    #end
  end

  # Suspend the autoscaling processes and either terminate or stop the EC2
  # instances under the autoscaling group.
  def stop(action)
    if !autoscaling_group.exists?
      @task.warn { "Autoscaling group #{@name} doesn't exist" }
      return
    end

    # FIXME: This won't work if we reinstate suspended processes...
    #if autoscaling_group.suspended_processes.empty?
      case action
      when :default, :terminate
        terminate_instances
      when :stop
        stop_instances
      else
        raise Cloud::Cycler::TaskFailure.new("Unrecognised autoscaling action #{action}")
      end
    #else
      #@task.debug { "Scaling group #{@name} already suspended" }
    #end
  end

  # Terminate all the EC2 instances under the autoscaling group.
  def terminate_instances
    @task.unsafe("Stopping #{@name} Launch process") do
      autoscaling_group.suspend_processes('Launch')
    end
    autoscaling_instances.each do |instance|
      @task.unsafe("Terminating instance #{instance.instance_id}") do
        load_balancers.each do |elb|
          elb.instances.deregister(instance.instance_id)
        end
        instance.ec2_instance.terminate
      end
    end
  end

  # Stop all the instances under the autoscaling group.
  # Normally, autoscaling instances should be safe to add/remove dynamically.
  # However, systems like CQ require manual intervention to add/remove
  # instances.
  def stop_instances
    @task.unsafe("Stopping #{@name} processes") do
      save_to_s3(@task.bucket)
      autoscaling_group.suspend_all_processes
    end
    autoscaling_instances.each do |instance|
      @task.unsafe("Stopping instance #{instance.instance_id}") do
        load_balancers.each do |elb|
          elb.instances.deregister(instance.instance_id)
        end
        instance.ec2_instance.stop
      end
    end
  end

  # Restart any stopped EC2 instances under the autoscaling group.
  def start_instances
    started = 0
    autoscaling_instances.each do |instance|
      ec2_instance = instance.ec2_instance
      next if !ec2_instance.exists?

      if ec2_instance.status == :stopped
        @task.unsafe("Starting instance #{instance.instance_id}") do
          ec2_instance.start
          load_balancers.each do |elb|
            elb.instances.register(instance.instance_id)
          end
          started += 1
        end
      else
        @task.debug { "Instance #{instance.instance_id} already running" }
      end
    end

    # FIXME
    # This is to give instances a little more time to start up and become
    # healthy before restarting autoscaling processes.
    # If an instance isn't started and healthy in time, the autoscale will kill
    # it for being unhealthy.
    #
    # The "right" way to do it would be to actually poll the instances until
    # they are healthy (or a timeout is reached). With the current task model,
    # other actions are blocked while this is waiting, so I can't afford to
    # wait too long.
    sleep(@grace_period) if started > 0
  end
  
  #
  # TODO: this is duplicated with cloudformation one, dry it up but don't understand it enough yet
  #
  # Save template and parameters to an S3 bucket
  # Bucket may be created if it doesn't exist
  def save_to_s3(bucket_name)
    suspended  = autoscaling_group.suspended_processes.to_h.to_json

    @task.unsafe("Writing #{@name} to bucket #{s3_bucket.name}") do
      s3_object("suspended.json").write(suspended)
    end
  end

  # Load template and parameters that were previously saved to an S3 bucket
  def load_from_s3(bucket)
    suspended = s3_object("suspended.json").read
    return JSON.parse(suspended)
  end

  private

  # AWS::AutoScaling object
  def aws_autoscaling
    @aws_autoscaling ||= AWS::AutoScaling.new(@task.aws_config)
  end

  # AWS::AutoScaling::Group object
  def autoscaling_group
    @autoscaling_group ||= aws_autoscaling.groups[@name]
  end

  # AWS::EC2::Instance objects contained by the scaling group.
  def autoscaling_instances
    autoscaling_group.auto_scaling_instances
  end

  def load_balancers
    autoscaling_group.load_balancers
  end

  # Memoization for S3 bucket object
  def s3_bucket
    return @s3_bucket if defined? @s3_bucket

    s3 = AWS::S3.new(@task.aws_config)
    @s3_bucket = s3.buckets[@task.bucket]
  end

  # Find an S3 object, prepending the task prefix, stack name, etc to the supplied path.
  def s3_object(path)
    @task.s3_object("asgroup/#{@name}/#{path}")
  end
end
