class Broker
  def initialize(partition_id, logger)
    @partition_id = partition_id
    @command_table = CommandTable::Broker.new(partition_id)
    @logger = logger
    @shutdown = false
  end

  def setup_signal_handlers
    trap('TERM') do
      log("Received TERM signal...")
      @shutdown = true
    end

    trap('INT') do
      log("Received INT signal...")
      @shutdown = true
    end
  end

  def run(schedule_batch_size=100, cleanup_batch_size=10)
    loop do
      sleep 1 # Sleeping on purpose to be able to follow logs

      if @shutdown
        log("Shutting down...")
        break
      end

      free_worker_ids = @command_table.fetch_free_worker_ids(schedule_batch_size)

      if free_worker_ids.empty?
        log("No free workers.")
      else
        log("Found #{free_worker_ids.size} free workers.")

        highest_priority_jobs = @command_table.next_highest_priority_jobs(free_worker_ids.size).map do |job_payload|
          build_job(job_payload)
        end
        log("Found #{highest_priority_jobs.size} highest priority jobs to schedule.")

        highest_priority_jobs.each do |next_job|
          log("Acquiring lock for job_id=#{next_job.id}")

          if @command_table.acquire_job_lock(next_job)
            log("Acquired lock for job_id=#{next_job.id}")
            free_worker_id = free_worker_ids.shift

            log("Assigning job_id=#{next_job.id} to worker_id=#{free_worker_id}")
            @command_table.schedule_job(next_job, @partition_id, free_worker_id)
          else
            log("Failed to acquire lock for job_id=#{next_job.id}. Discarding...")
            @command_table.discard_job(next_job)
          end
        end
      end

      dead_workers = @command_table.fetch_dead_workers(cleanup_batch_size)
      if dead_workers.empty?
        log("No dead workers to recover.")
      else
        log("Found #{dead_workers.size} dead workers to recover...")

        dead_workers.each do |dead_worker_id|
          @command_table.recover_in_progress_job(dead_worker_id)
        end

        @command_table.clear_workers(dead_workers)
        log("Recovered dead workers.")
      end
    end
  end

  private

  def log(message)
    @logger.info("[Hedwig Broker partition_id=#{@partition_id}] ".green + message)
  end

  def build_job(serialized_payload)
    deserialized_payload = JSON.parse(serialized_payload)

    Job.new(
      priority: deserialized_payload.fetch("priority"),
      id: deserialized_payload.fetch("id"),
      logger: @logger
    )
  end
end
