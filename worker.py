#!/usr/bin/python

import swf_helper
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-t", "--task-list", type=str, dest="task_list", default="A", help="task list name", metavar="task_list")
args = parser.parse_args()

print "Listening for Worker Tasks"

while True:

    task = swf_helper.poll_for_activity_task(args.task_list)

    task_input = int(task['input'])
    result = str(task_input + 1)
    print "New task arrived: {} -> {}".format(task_input, result)

    swf_helper.respond_activity_task_completed(task=task, result=result)

