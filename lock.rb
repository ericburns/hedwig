class Lock
  def initialize(id, ttl, concurrency)
    @id = id
    @ttl = ttl
    @concurrency = concurrency
  end
end
