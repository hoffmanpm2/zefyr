require 'open3'
require_relative 'zfs'

module Zefyr
  class Backup

    def initialize (source, target, options)
      @options = options
      @source = source
      @target = target
    end

    def execute
      zfs_source = Zefyr::ZFS.new(@source, @options)
      zfs_target = Zefyr::ZFS.new(@target, @options)

      # Retrieve filesystems from source to transfer to target.
      if @options[:children]
        zfs_source.get_filesystems.each do |filesystem|

          # Create filesystem on target if it does not exist.
          target_filesystem = @target.filesystem.nil? ? filesystem :
              @target.filesystem.gsub(filesystem, @target.filesystem)
          zfs_target.create_filesystem unless zfs_target.has_filesystem?

          # Retrieve the snapshots for source & target filesystems
          source_snapshots = zfs_source.get_snapshots(filesystem)
          target_snapshots = zfs_target.get_snapshots(filesystem)

          # Determine which snapshots need to be transferred to the target.
          # Create the snapshot if the user indicates the desire to do so.
          snapshots = nil
          if @source.snapshot.nil?
            if @options[:recursive]
              snapshots = source_snapshots - target_snapshots
            else
              # Transfer last snapshot
            end
          elsif zfs_source.has_snapshot(filesystem) != @options[:create_snapshot]
            if @options[:recursive]
              snapshots = source_snapshots - target_snapshots
            else
              snapshots = @source.snapshot
            end

            if @options[:create_snapshot]
              zfs_source.create_snapshot(filesystem)
              snapshots.push(@source.snapshot) if @options[:recursive]
            end
          else
            if @options[:create_snapshot]
              STDERR.puts "Snapshot already exists."
            else
              STDERR.puts "Unknown snapshot: #{@source.snapshot}. Use -s flag to create snapshot."
            end
          end

          if @options[:recursive]
            previous_snapshot = nil
            snapshots.each do |snapshot|
              # TODO: Automatically determine the last snapshot and transfer
              # an incremental snapshot instead of the full. This snapshot could
              # be the previous snapshot transferred if transferring multiple
              # snapshots.

              # If target doesn't have snapshots or source & target do not share a
              # common snapshot then transfer first snapshot.
              if target_snapshots.nil? || (snapshots - source_snapshots).nil?
                zfs_source.copy_snapshot(zfs_target, snapshot)
              else
                zfs_source.copy_incremental_snapshot(zfs_target, snapshot, previous_snapshot)
              end

              previous_snapshot = snapshot
            end
          end

          # Remove all snapshots that are not present in source filesystems
          if @options[:destroy]
            obsolete_snapshots = target_snapshots - source_snapshots
            obsolete_snapshots.each do |snapshot|
              zfs_target.destroy_snapshot(snapshot)
            end
          end
        end
      else

      end
    end

  end
end