#!/usr/bin/python

import boto3

swf = boto3.client('swf')

DOMAIN = "rmechler_test"
WORKFLOW = "rmechler_test_workflow"
TASKNAME = "rmechler_test_task"
VERSION = "0.1"
TASKLIST = "rmechler_test_tasklist"

response = swf.start_workflow_execution(
  domain=DOMAIN,
  workflowId='test-2003',
  workflowType={
    "name": WORKFLOW,
    "version": VERSION
  },
  taskList={
      'name': TASKLIST
  },
  input=''
)

print "Workflow requested: ", response


