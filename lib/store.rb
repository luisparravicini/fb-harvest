require 'sqlite3'

OUT_DIR = 'out'

module BaseStore
  def self.open_db(fname)
    unless File.exists?(fname)
      FileUtils.cp(fname.gsub(/\.db/, '_empty.db'), fname)
    end
    SQLite3::Database.new(fname)
  end
end

class PeopleStore
  def initialize
    @db = BaseStore.open_db('people.db')
  end

  def update(leaves)
    people = update_db(leaves)
    dump_to_disk

    people
  end

  def update_db(leaves)
    people = 0

    @db.transaction do |db|
      unless leaves.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO people (url, name) VALUES(?,?)')
        leaves.each do |leaf|
          stmt.execute(leaf[:url], leaf[:title])
          people += @db.changes
        end
      end
    end

    people
  end

  def dump_to_disk
    return if people_size < 1000000

    result = @db.execute('SELECT url, name FROM people')
    fname = "#{OUT_DIR}/directory-#{Time.now.to_i}"
    FileUtils.mkdir_p(OUT_DIR) unless File.directory?(OUT_DIR)
    raise "file exists! #{fname}" if File.exists?(fname)
    File.open(fname, 'w') do |io|
      result.each { |r| io.puts r.join("\t") }
    end
    @db.execute("DELETE FROM people")
    @db.execute("VACUUM");
  end

  def people_size
    @db.execute('SELECT COUNT(1) FROM people').first.first
  end

end

class QueueStore
  def initialize
    @db = BaseStore.open_db('queue.db')
  end

  def reset_status
    @db.execute("UPDATE queue SET status=NULL WHERE status=0")
  end

  def next
    result = @db.execute('SELECT id, url FROM queue WHERE status IS NULL LIMIT 1')
    unless result.empty?
      result.first.tap do |r|
        @db.execute("UPDATE queue SET status=0 WHERE id=#{r.first}")
      end
    end
  end

  def update(id, nodes)
    queue = 0

    @db.transaction do |db|
      unless nodes.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO queue (url) VALUES(?)')
        nodes.sort_by { rand }.each do |node|
          stmt.execute(node[:url])
          queue += @db.changes
        end
      end

      finish(id)
    end

    queue
  end

  def finish(id)
    @db.execute("UPDATE queue SET status=1 WHERE id=#{id}") unless id.nil?
  end

  def queue_size
    @db.execute('SELECT COUNT(1) FROM queue WHERE status IS NULL').first.first
  end
end

class Store
  def initialize
    @queue = QueueStore.new
    @people = PeopleStore.new
  end

  def reset_status
    @queue.reset_status
  end

  def update(id, nodes, leaves)
    [ @queue.update(id, nodes), @people.update(leaves) ]
  end

  def next; @queue.next; end

  def queue_size; @queue.queue_size; end
end

