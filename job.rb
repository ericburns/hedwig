class Job
  attr_reader :priority, :id, :lock

  def initialize(priority:, id: rand(1000000), lock: nil, logger:)
    @priority = priority
    @id = id
    @lock = lock
    @logger = logger
  end

  def perform
    @logger.info("[Hedwig Job id=#{@id} priority=#{@priority}] Done.".magenta)
  end

  def to_json
    {
      priority: @priority,
      id: @id,
    }.to_json
  end
end
