module Zefyr
  class ZFS
    attr_reader :path, :operand, :options

    def initialize(operand, options)
      @operand = operand
      @options = options
      @path = get_zfs_path()
    end

    def copy_incremental_snapshot (filesystem, snapshot)
      # zfs send -i old_snapshot filesystem@snapshot | zfs recv
    end

    def copy_snapshot (filesystem, snapshot)
      # zfs send
      send_command = source.nil? ? "#{source_zfs} send #{}" : ""
    end

    def create_filesystem ()
      zfs_command = "#{@path} create #{@operand.filesystem}"
      command = build_command (zfs_command)
      execute_command(command)
    end

    def create_snapshot (filesystem)
      zfs_command = "#{@path} snapshot #{filesystem}@#{@operand.snapshot}"
      command = build_command (zfs_command)
      execute_command(command)
    end

    def destroy_snapshot (snapshot)
      zfs_command = "#{@path} destroy #{snapshot}"
      command = build_command (zfs_command)
      execute_command(command)
    end

    def get_filesystems ()
      zfs_command = options[:recursive] ? "#{@path} list -Ho name -s name | grep -e '#{@operand.filesystem}'"
      : "#{@path} list -Ho name | grep -e '#{@operand.filesystem}$'"
      command = build_command (zfs_command)
      execute_complex_query(command)
    end

    def get_snapshots (filesystem)
      zfs_command = "#{@path} list -H -t snapshot -o name -s creation | grep -e '#{filesystem}@'"
      command = build_command (zfs_command)
      execute_complex_query(command)
    end

    def has_filesystem? ()
      zfs_command = "#{@path} list -Ho name | grep -e #{@operand.filesystem}"
      command = build_command (zfs_command)
      !execute_simple_query(command).empty?
    end

    def has_snapshot? (filesystem)
      zfs_command = "#{@path} list -Ho name -t snapshot | grep -e '#{filesystem}@#{@operand.snapshot}$'"
      command = build_command (zfs_command)
      !execute_simple_query(command).empty?
    end

    private

    def build_command (command)
      unless @operand.host.nil?
        command = options[:rsh].nil? ?
            "ssh #{@operand.host} #{command}" :
            "#{@options[:rsh]} #{@operand.host} #{command}"
      end
      command
    end

    def execute_command (command)
      status = Open4::popen4 (command) do | pid, stdin, stdout, stderr |
        error = stderr.read.strip
        STDERR.puts error unless error.empty?
      end
      status.exitstatus == 0
    end

    def execute_complex_query (command)
      status = Open4::popen4 (command) do | pid, stdin, stdout, stderr |
        error = stderr.read.strip
        STDERR.puts error unless error.empty?
        output = stdout.read.lines.map(&:chomp) if error.empty?
      end
      output
    end

    def execute_simple_query (command)
      status = Open4::popen4 (command) do | pid, stdin, stdout, stderr |
        error = stderr.read.strip
        STDERR.puts error unless error.empty?
        output = stdout.read.strip if error.empty?
      end
      output
    end

    def get_zfs_path ()
      command = build_command ("which zfs")
      execute_simple_query(command)
    end
  end
end
