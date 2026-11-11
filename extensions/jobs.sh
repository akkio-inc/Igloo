# shellcheck shell=bash
# Sourced by the extensions' build scripts. build_jobs BYTES prints how many
# compiles to run at once: as many as there is memory for at BYTES each,
# counting the memory available and any cgroup limit (a container's), at
# least one and at most one a core. A compiler run out of memory is killed,
# and the build with it: a VM of 6 GB could not build either extension with
# a compile a core, or all of rb's at once.
build_jobs() {
	local free room jobs
	free=$(($(awk '/^MemAvailable:/ { print $2 }' /proc/meminfo) * 1024))
	if [ -r /sys/fs/cgroup/memory.max ] && [ "$(cat /sys/fs/cgroup/memory.max)" != max ]; then
		room=$(($(cat /sys/fs/cgroup/memory.max) - $(cat /sys/fs/cgroup/memory.current)))
		if [ "$room" -lt "$free" ]; then free=$room; fi
	fi
	jobs=$((free / $1))
	if [ "$jobs" -gt "$(nproc)" ]; then jobs=$(nproc); fi
	if [ "$jobs" -lt 1 ]; then jobs=1; fi
	echo "$jobs"
}
