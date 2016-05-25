#!/usr/bin/python

import swf_helper
import logging
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-w", "--workflow-id", type=str, dest="workflow_id", default="test-1", help="workflow ID", metavar="workflow_id")
args = parser.parse_args()

WORKFLOW_ID = 'test-1'

response = swf_helper.start_workflow(args.workflow_id, workflowTimeout=180, deciderTimeout=10)

print("Workflow requested: {}".format(response))
