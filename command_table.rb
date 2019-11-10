

module CommandTable
  BUSINESS_TABLE_KEY = "busy"
  BUSINESS_TABLE_FREE_FLAG = 0
  HEARTBEAT_TABLE_KEY = "heartbeat"
  PRIORITIES_KEY = "priorities"

  class Broker
    def initialize(partition_id)
      raise ArgumentError, "partition_id must be greater or equal to 1" if partition_id < 1

      @partition_id = partition_id
      @metadata_redis = Redis.new(url: "redis://localhost:6379/0")
      @jobs_redis = Redis.new(url: "redis://localhost:6379/#{partition_id}")
    end

    def next_highest_priority_jobs(n)
      priorities = @jobs_redis.zrevrange(PRIORITIES_KEY, 0, -1)
      return [] if priorities.empty?

      highest_priority = priorities.first
      priorities.each do |priority|
        break unless @jobs_redis.zcard("queue:#{highest_priority}").zero?
        highest_priority = priority
      end

      @jobs_redis.zrangebyscore("queue:#{highest_priority}", "-inf", Time.now.to_f, limit: [0, n])
    end

    def schedule_job(job, partition_id, worker_id)
      @metadata_redis.zadd(BUSINESS_TABLE_KEY, partition_id.to_f, worker_id)

      @jobs_redis.pipelined do
        @jobs_redis.zrem("queue:#{job.priority}", job.to_json)
        @jobs_redis.set("in_progress:#{worker_id}", job.to_json)
      end
    end

    def discard_job(job)
      @jobs_redis.zrem("queue:#{job.priority}", job.to_json)
    end

    def fetch_free_worker_ids(n)
      @metadata_redis.zrangebyscore(BUSINESS_TABLE_KEY, BUSINESS_TABLE_FREE_FLAG, BUSINESS_TABLE_FREE_FLAG, limit: [0, n])
    end

    def fetch_in_progress_job(worker_id)
      @jobs_redis.get(worker_id)
    end

    def recover_in_progress_job(worker_id)
      json_job = fetch_in_progress_job(worker_id)

      if json_job
        priority = JSON.parse(job).fetch("priority")
        @jobs_redis.zadd("queue:#{priority}", Time.now.to_f, json_job)
      end
    end

    def acquire_job_lock(job)
      true
    end

    def fetch_dead_workers(ttl=30)
      @metadata_redis.zrangebyscore(HEARTBEAT_TABLE_KEY, -Float::INFINITY, (Time.now - 30).to_f)
    end

    def clear_workers(workers)
      @metadata_redis.pipelined do
        @metadata_redis.zrem(BUSINESS_TABLE_KEY, workers)
        @metadata_redis.zrem(HEARTBEAT_TABLE_KEY, workers)
      end
    end
  end

  class Worker
    def initialize
      @metadata_redis = Redis.new(url: "redis://localhost:6379/0")

      @jobs_redis_partition_count = 3
      @jobs_redis_partitions = {}
      (1..@jobs_redis_partition_count).each do |partition_id|
        @jobs_redis_partitions[partition_id] = Redis.new(url: "redis://localhost:6379/#{partition_id}")
      end
    end

    def enqueue_job(job, time=Time.now)
      random_jobs_redis.zadd(PRIORITIES_KEY, job.priority.to_f, job.priority)
      random_jobs_redis.zadd("queue:#{job.priority}", time.to_f, job.to_json)
    end

    def get_job(worker_id)
      partition_id = random_jobs_redis_partition_id

      [@jobs_redis_partitions.fetch(partition_id).get("in_progress:#{worker_id}"), partition_id]
    end

    def acknowledge(worker_id, partition_id, lock=nil)
      @jobs_redis_partitions[partition_id].del("in_progress:#{worker_id}")

      @metadata_redis.zadd(BUSINESS_TABLE_KEY, BUSINESS_TABLE_FREE_FLAG, worker_id)
      release_job_lock(lock) if lock
    end

    def heartbeat(worker_id)
      @metadata_redis.zadd(HEARTBEAT_TABLE_KEY, Time.now.to_f, worker_id)
    end

    def clear_heartbeat(worker_id)
      @metadata_redis.zrem(BUSINESS_TABLE_KEY, worker_id)
      @metadata_redis.zrem(HEARTBEAT_TABLE_KEY, worker_id)
    end

    def release_job_lock(lock)
      true
    end

    private

    def random_jobs_redis_partition_id
      (1..@jobs_redis_partition_count).to_a.sample
    end

    def random_jobs_redis
      @jobs_redis_partitions.fetch(random_jobs_redis_partition_id)
    end
  end
end
