#!/usr/bin/python

import boto3

swf = boto3.client('swf')

DOMAIN = "rmechler_test"
WORKFLOW = "rmechler_test_workflow"
TASKNAME = "rmechler_test_task"
VERSION = "0.1"
TASKLIST = "rmechler_test_tasklist"

WORKFLOW_ID = 'test-2001'


# response = client.terminate_workflow_execution(
#     domain=DOMAIN,
#     workflowId=WORKFLOW_ID,
#     runId='string',
#     reason='string',
#     details='string',
#     childPolicy='TERMINATE'|'REQUEST_CANCEL'|'ABANDON'
# )

response = swf.start_workflow_execution(
  domain=DOMAIN,
  workflowId=WORKFLOW_ID,
  workflowType={
    "name": WORKFLOW,
    "version": VERSION
  },
  taskList={
      'name': TASKLIST
  },
  taskStartToCloseTimeout='10',
  input=''
)

print "Workflow requested: ", response


