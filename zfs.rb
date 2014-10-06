require 'open4'

module Zefyr
  class ZFS
    attr_reader :path, :operand, :options

    def initialize(operand, options)
      @operand = operand
      @options = options
      @path = get_zfs_path()
    end

    def copy_incremental_snapshot (target, snapshot, filesystem)
      create_snapshot if @options[:snapshot]
      unless has_snapshot? || @options[:dry_run]
        STDERR.puts "The specified snapshot, #{@operand.filesystem}@#{@operand.snapshot}, does not exist."
        return false
      end

      send_command = "#{@path} send "
      send_command += "-R " if @options[:children] && @options[:destroy]
      send_command += "-p " if @options[:preserve_properties]
      send_command += @options[:recursive] ? "-I #{snapshot} " : "-i #{snapshot} "
      send_command += filesystem.nil? ? "#{@operand.filesystem}@#{@operand.snapshot}"
        : "#{filesystem}@#{@operand.snapshot}"
      send_command = build_command (send_command)

      recv_command = filesystem.nil? ? "#{target.path} recv -F #{target.get_filesystem}"
        : "#{target.path} recv -F #{filesystem.gsub(@operand.filesystem, target.get_filesystem)}"
      recv_command = target.build_command(recv_command)
      command = "#{send_command} | #{recv_command}"

      if @options[:dry_run]
        puts command
      else
        execute_command(command)
      end
    end

    def copy_snapshot (target, filesystem)
      create_snapshot if @options[:snapshot]
      unless has_snapshot? || @options[:dry_run]
        STDERR.puts "The specified snapshot, #{@operand.filesystem}@#{@operand.snapshot}, does not exist."
        return false
      end

      unless @options[:recursive] && (!@options[:children] || !@options[:destroy])
        send_command = "#{@path} send "
        send_command += "-R " if @options[:children] && @options[:destroy]
        send_command += "-p " if @options[:preserve_properties]
        send_command += "#{@operand.filesystem}@#{@operand.snapshot}"
        send_command = build_command (send_command)

        recv_command = "#{target.path} recv -F #{target.get_filesystem}"
        recv_command = target.build_command(recv_command)
        command = "#{send_command} | #{recv_command}"

        if @options[:dry_run]
          puts command
        else
          execute_command(command)
        end
      else
        # Send first snapshot to create filesystem & stream package that
        # sends all intermediary snapshots.
        snapshots = get_snapshots(@operand.filesystem)
        unless snapshots.nil?
          init_snapshot = snapshots.first
          send_command = "#{@path} send "
          send_command += "-p " if @options[:preserve_properties]
          send_command += "#{init_snapshot}"
          send_command = build_command(send_command)

          recv_command = "#{target.path} recv -F #{target.get_filesystem}"
          recv_command = target.build_command(recv_command)
          command = "#{send_command} | #{recv_command}"

          send_command = "#{@path} send "
          send_command += "-p " if @options[:preserve_properties]
          send_command += "-I #{init_snapshot} #{@operand.filesystem}@#{@operand.snapshot}"
          send_command = build_command(send_command)
          command = "#{command} && #{send_command} | #{recv_command}"

          if @options[:dry_run]
            puts command
          else
            execute_command(command)
          end
        end
      end
    end

    def create_snapshot ()
      if !has_snapshot?
        zfs_command = @options[:children] ? "#{@path} snapshot -r #{@operand.filesystem}@#{@operand.snapshot}" :
          "#{@path} snapshot #{@operand.filesystem}@#{@operand.snapshot}"
        command = build_command (zfs_command)

        if @options[:dry_run]
          puts command
        else
          execute_command(command)
        end
      end
    end

    def destroy_snapshot (snapshot)
      zfs_command = "#{@path} destroy #{snapshot}"
      command = build_command (zfs_command)

      if @options[:dry_run]
        puts command
      else
        execute_command(command)
      end
    end

    def get_filesystem ()
      @operand.filesystem
    end

    def get_filesystems ()
      zfs_command = options[:children] ? "#{@path} list -Ho name -s name | grep -e '#{@operand.filesystem}'"
      : "#{@path} list -Ho name | grep -e '#{@operand.filesystem}$'"
      command = build_command (zfs_command)
      execute_complex_query(command)
    end

    def get_snapshots (filesystem)
      zfs_command = "#{@path} list -H -t snapshot -o name -s creation | grep -e '#{filesystem}@'"
      command = build_command (zfs_command)
      execute_complex_query(command)
    end

    def has_filesystem? (filesystem)
      zfs_command = "#{@path} list -Ho name | grep -e #{filesystem}"
      command = build_command (zfs_command)
      !execute_simple_query(command).empty?
    end

    def has_snapshot? ()
      zfs_command = "#{@path} list -Ho name -t snapshot | grep -e '#{@operand.filesystem}@#{@operand.snapshot}$'"
      command = build_command (zfs_command)
      !execute_simple_query(command).empty?
    end

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
      output = nil
      status = Open4::popen4 (command) do | pid, stdin, stdout, stderr |
        error = stderr.read.strip
        STDERR.puts error unless error.empty?
        output = stdout.read.lines.map(&:chomp) if error.empty?
      end
      output
    end

    def execute_simple_query (command)
      output = nil
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
