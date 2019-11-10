class Worker
  attr_reader :id

  def initialize(logger)
    @command_table = CommandTable::Worker.new
    @id = rand(10000)
    @logger = logger
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

  def run
    loop do
      @command_table.heartbeat(@id)

      if @shutdown
        log("Shutting down...")
        break
      end

      log("Fetching job...")
      job_payload, partition_id = get_job

      if job_payload
        job = build_job(job_payload)
        log("Found job partition_id=#{partition_id} job_id=#{job.id} priority=#{job.priority}")

        job.perform
        acknowledge(partition_id)
        release_job_lock(job.lock) if job.lock
      else
        log("No job found. Sleeping for a second...")
        sleep 1 # Sleeping on purpose to be able to follow logs
        acknowledge(partition_id)
      end
    end
  end

  private

  def log(message)
    @logger.info("[Hedwig Worker id=#{@id}] ".light_blue + message)
  end

  def build_job(serialized_payload)
    deserialized_payload = JSON.parse(serialized_payload)

    Job.new(
      priority: deserialized_payload.fetch("priority"),
      id: deserialized_payload.fetch("id"),
      logger: @logger
    )
  end

  def get_job
    @command_table.get_job(@id)
  end

  def acknowledge(partition_id)
    @command_table.acknowledge(@id, partition_id, nil)
  end

  def heartbeat
    @command_table.heartbeat(@id)
  end
end
