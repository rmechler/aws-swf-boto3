#!/usr/bin/python

import boto3
from botocore.client import Config

botoConfig = Config(connect_timeout=50, read_timeout=70)
swf = boto3.client('swf', config=botoConfig)

DOMAIN = "rmechler_test"
WORKFLOW = "rmechler_test_workflow"
TASKNAME = "rmechler_test_task"
VERSION = "0.1"
TASKLIST = "rmechler_test_tasklist"

print "Listening for Worker Tasks"

while True:

  task = swf.poll_for_activity_task(
    domain=DOMAIN,
    taskList={'name': TASKLIST},
    identity='worker-1')

  if 'taskToken' not in task:
    print "Poll timed out, no new task.  Repoll"

  else:
    task_input = int(task['input'])
    result = str(task_input + 1)
    print "New task arrived: {} -> {}".format(task_input, result)

    swf.respond_activity_task_completed(
        taskToken=task['taskToken'],
        result=result
    )

    print "Task Done"


