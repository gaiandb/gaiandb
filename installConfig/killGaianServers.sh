#!/bin/bash
# ============================================================================
#  GaianDB
#  Copyright IBM Corp. 2007-2014
#  
#  LICENSE: Eclipse Public License v1.0
#  http://www.eclipse.org/legal/epl-v10.html
# ============================================================================

USAGE="`basename $0` [-signal_number] <process_name>"

#pattern=$*
pattern=*com.ibm.gaiandb.GaianNode

if [[ -z $pattern ]]; then
	echo $USAGE
	echo "Kills all occurences of a process by name (matched exactly - including args)."
	echo "Lists PIDs then waits for user input confirmation before kill is issued."
	exit 1
fi


SIGNAL=-15
if [[ $1 = -[0-9]* ]]; then
	SIGNAL=$1
	shift
fi

if [[ -z $2 ]]; then        # all matches starting with $1
	PAT=`ps -ef | grep -E "[/| ]$pattern" | grep -vE "[^ ]* $$" | grep -v "grep -E"`
else
if [[ $1 = -e* ]]; then     # exact match
	shift
	PAT=`ps -ef | grep -E "[/| ]$pattern $" | grep -vE "[^ ]* $$" | grep -v "grep -E"`
fi
fi

# | grep -v '/*kall '`
#PAT=`ps -e | grep " $pattern$"`

if [[ -z $PAT ]]; then
	echo "$pattern: Not running"
	exit 0
fi

PIDS=`echo "$PAT" | awk '{print $2}'`
#PROCS=`echo "$PAT" | awk '{print $8}' | sort | uniq`

PROCS=`echo "$PAT" | sed "s,.* \($pattern\) $,\1," | sort | uniq`

count=`echo "$PAT" | grep -c ""`

#echo "$PAT"

CMD="kill $SIGNAL $PIDS"
printf "Sending signal $SIGNAL to $count instances of $pattern:\n$PROCS\npids: "
echo $PIDS
[[ $* = *noprompt ]] || ( echo "Press Ctrl^C to abort - any other to continue" && read )
$CMD
