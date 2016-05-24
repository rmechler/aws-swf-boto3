#!/usr/bin/python

import boto3
from botocore.exceptions import ClientError

DOMAIN = "rmechler_test"
WORKFLOW = "rmechler_test_workflow_new"
TASKNAME = "rmechler_test_task"
VERSION = "0.1"
TASKLIST = "rmechler_test_tasklist"

swf = boto3.client('swf')

try: 
  swf.register_domain(
    name=DOMAIN,
    description="Test SWF domain",
    workflowExecutionRetentionPeriodInDays="10"
  )
except ClientError as e:
  print "Domain already exists: ", e.response.get("Error", {}).get("Code")

try:
  swf.register_workflow_type(
    domain=DOMAIN,
    name=WORKFLOW,
    version=VERSION,
    description="Test workflow",
    defaultExecutionStartToCloseTimeout="250",
    defaultTaskStartToCloseTimeout="15",
    defaultChildPolicy="TERMINATE",
    defaultTaskList={"name": TASKLIST}
  )
  print "Test workflow created!"
except ClientError as e:
  print "Workflow already exists: ", e.response.get("Error", {}).get("Code")

try:
  swf.register_activity_type(
    domain=DOMAIN,
    name=TASKNAME,
    version=VERSION,
    description="Test worker",
    defaultTaskStartToCloseTimeout="NONE",
    defaultTaskList={"name": TASKLIST}
  )
  print "Test worker created!"
except ClientError as e:
  print "Activity already exists: ", e.response.get("Error", {}).get("Code")

