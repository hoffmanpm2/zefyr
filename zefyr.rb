#!/usr/bin/env ruby
#
# zefyr backup [-crvdnpsh] [-e ssh_options] [USER@]HOST:FS[@SNAP] [USER@]HOST:FS
#   -c : Transfer child filesystems
#	-r : Transfer snapshots up to and including specified snapshot. If none specified, transfer all.
#	-v : verbose output
#	-d : delete snapshots on client that are not present on host.
#	-n : dry run
#	-p : preserve properties
#	-s : create new snapshot.
#	-e : remote shell to use
#	-h : display usage information
#
# zefyr restore [-cvnph] [-e ssh_options] [USER@]HOST:FS[@SNAP] [USER@]HOST:FS
#	-c : Recursively restore all child filesystems.
#	-v : verbose output
#	-n : dry run
#	-p : preserve properties
#	-e : shell command to use
#	-h : display usage information
#

require 'optparse'
require_relative 'backup'
#require_relative 'restore'

module Zefyr
	class Operand
		attr_reader :host, :filesystem, :snapshot

		def initialize (operand)
			if /(?<host>.+):(?<filesystem>.+)@(?<snapshot>.+)/ =~ operand
				@host = host
				@filesystem = filesystem
				@snapshot = snapshot
			elsif /(?<host>.+):(?<filesystem>.+)/ =~ operand
				@host = host
				@filesystem = filesystem
				@snapshot = nil
			elsif /(?<filesystem>.+)@(?<snapshot>.+)/ =~ operand
				@host = nil
				@filesystem = filesystem
				@snapshot = snapshot
      else
        @host = nil
				@filesystem = operand
        @snapshot = nil
			end
		end

		def to_s ()
			"Host: #{host}\nFilesystem: #{filesystem}\nSnapshot: #{snapshot}\n"
		end
	end

    def self.remove_leading_slash (filesystem)
      filesystem =~ /^\// ? filesystem[1..-1] : filesystem
    end

    def self.remove_trailing_slash (filesystem)
      filesystem =~ /\/$/ ? filesystem.chop : filesystem
    end
end


options = {}
syntax = "Syntax: zefyr backup [-crvdnps] [-e command] [USER@][HOST:]FS[@SNAP] [USER@]HOST
	zefyr restore [-cvnp] [-e command] [USER@]HOST:FS[@SNAP] [USER@]HOST"

# Parse command-line options
option_parser = OptionParser.new do |opts|
	# Set a banner, displayed at the top of the help screen.
	opts.banner = syntax

	options[:children] = false
	opts.on('-c', '--children', "Copy filesystems recursively.") do
		options[:children] = true
	end

	options[:recursive] = false
	opts.on('-r', '--recursive', "Copy snapshots recursively.") do
		options[:recursive] = true
	end

	options[:verbose] = false
	opts.on('-v', '--verbose', "Verbose output.") do
		options[:verbose] = true
	end

	options[:destroy] = false
	opts.on('-d', '--destroy', "Destroy obsolete snapshots.") do
		options[:destroy] = true
	end

	options[:dry_run] = false
	opts.on('-n', '--dry-run', "Dry run.") do
		options[:dry_run] = true
	end

	options[:preserve_properties] = false
	opts.on('-p', '--preserve-properties', "Preserve filesystem properties.") do
		options[:preserve_properties] = true
	end

	options[:snapshot] = false
	opts.on('-s', '--snapshot', "Create new snapshot.") do
		options[:snapshot] = true
	end

	options[:rsh] = nil
	opts.on('-e', '--rsh COMMAND', "Shell command to use.") do |command|
		options[:rsh] = command
	end

	opts.on('-h', '--help', "Display this screen.") do
		puts opts
		exit
	end
end
option_parser.parse!

# Parse command-line operands
unless ARGV.empty?
	subcommand = ARGV.shift
	unless subcommand == 'backup' or subcommand == 'restore'
		puts "Unknown operand: '#{subcommand}'"
		puts syntax
		exit
	end

	source = nil
	target = nil
	if ARGV.length == 2
		source = Zefyr::Operand.new(ARGV.shift)
		target = Zefyr::Operand.new(ARGV.shift)
		puts "Source:\n#{source}"
		puts "Target:\n#{target}"

		# Create backup or restore object here
		operation = subcommand == 'backup' ? Zefyr::Backup.new(source, target, options) :
			Zefyr::Restore.new(source, target, options)
		operation.execute
	elsif ARGV.length < 2
		puts "Error: 'Too few operands'"
		puts syntax
	else
		puts "Error: 'Too many operands'"
		puts syntax
	end
else
	puts "Error: Missing subcommand."
	puts syntax
end