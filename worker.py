#!/usr/bin/python

import swf_helper

print "Listening for Worker Tasks"

while True:

    task = swf_helper.poll_for_activity_task('A')

    task_input = int(task['input'])
    result = str(task_input + 1)
    print "New task arrived: {} -> {}".format(task_input, result)

    swf_helper.respond_activity_task_completed(task=task, result=result)

