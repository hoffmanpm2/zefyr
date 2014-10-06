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

      # Loop thru filesystems. Cannot use replication stream unless user
      # specifies that nonexistent snapshots can be removed.
      if @options[:children] && !@options[:destroy]
        zfs_source.get_filesystems.each do |filesystem|
          source_snapshots = zfs_source.get_snapshots(filesystem) if zfs_source.has_filesystem?(filesystem)
          target_snapshots = zfs_target.get_snapshots(filesystem) if zfs_target.has_filesystem?(filesystem)

          # Get latest snapshot that filesystems have in common
          snapshot = (source_snapshots & target_snapshots).pop unless source_snapshots.nil? || target_snapshots.nil?

          # Copy snapshot to target
          snapshot_copied = snapshot.nil? ? zfs_source.copy_snapshot(zfs_target, filesystem) :
            zfs_source.copy_incremental_snapshot(zfs_target, snapshot, filesystem)
        end
      else
        # Retrieve snapshots from both filesystems
        source_snapshots = zfs_source.get_snapshots(@source.filesystem) if zfs_source.has_filesystem?(@source.filesystem)
        target_snapshots = zfs_target.get_snapshots(@source.filesystem) if zfs_target.has_filesystem?(@source.filesystem)

        # Get latest snapshot that filesystems have in common
        snapshot = (source_snapshots & target_snapshots).pop unless source_snapshots.nil? || target_snapshots.nil?

        # Copy snapshot to target
        snapshot_copied = snapshot.nil? ? zfs_source.copy_snapshot(zfs_target) : 
          zfs_source.copy_incremental_snapshot(zfs_target, snapshot)

        unless snapshot_copied || target_snapshots.nil?
          # Destroy snapshots on target filesystem that are not present
          # on source filesystem.
          if @options[:destroy]
            snapshots = target_snapshots - source_snapshots
            snapshots.each { |snapshot| zfs_target.destroy_snapshot(snapshot) }
          end
        end
      end
    end
  end
end