#!/usr/bin/python

import boto3
from botocore.client import Config
import uuid
import json
import datetime
import time

botoConfig = Config(connect_timeout=50, read_timeout=70)
swf = boto3.client('swf', config=botoConfig)

DOMAIN = "rmechler_test"
WORKFLOW = "rmechler_test_workflow"
TASKNAME = "rmechler_test_task"
VERSION = "0.1"
TASKLIST = "rmechler_test_tasklist"


outstandingTasks = []

print "Listening for Decision Tasks"

count = 0
seen_events = []

def default(o):
    if type(o) is datetime.date or type(o) is datetime.datetime:
        return o.isoformat()


def schedule_activity_task(data):
  print("ScheduleActivityTask: {}".format(data))
  return {
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': {
                'activityType':{
                    'name': TASKNAME,
                    'version': VERSION
                    },
                'activityId': 'activityid-' + str(uuid.uuid4()),
                'input': data,
                'scheduleToCloseTimeout': 'NONE',
                'scheduleToStartTimeout': 'NONE',
                'startToCloseTimeout': 'NONE',
                'heartbeatTimeout': 'NONE',
                'taskList': {'name': TASKLIST},
            }
          }

while True:

  newTask = swf.poll_for_decision_task(
    domain=DOMAIN,
    taskList={'name': TASKLIST},
    identity='decider-1',
    reverseOrder=True)

  if 'taskToken' not in newTask:
    print "Poll timed out, no new task.  Repoll"

  elif 'events' in newTask:

    count = count + 1

    time.sleep(1)

    # new_events = [evt for evt in newTask['events'] if not evt['eventId'] in seen_events]

    # for evt in new_events:
    #   seen_events.append(evt['eventId'])

    # with open("tasks.{}".format(count), "w") as f:
    #   f.write(json.dumps(newTask, indent=2, default=default))

    # with open("events.{}".format(count), "w") as f:
    #   f.write(json.dumps(new_events, indent=2, default=default))

    print("events {} to {}".format(newTask['previousStartedEventId'] + 1, newTask['startedEventId']))

    newEvents = [evt for evt in newTask['events'] if evt['eventId'] <= newTask['startedEventId'] and evt['eventId'] > newTask['previousStartedEventId']]
    
    events = [evt for evt in newEvents if not evt['eventType'].startswith('Decision')]

    # lastEvent = eventHistory[0]

    decisions = []
    for lastEvent in events:

      if lastEvent['eventType'] == 'WorkflowExecutionStarted' and newTask['taskToken'] not in outstandingTasks:
        print "Dispatching task to worker", newTask['workflowExecution'], newTask['workflowType']
        decisions.append(schedule_activity_task('1'))
        decisions.append(schedule_activity_task('1000'))

      elif lastEvent['eventType'] == 'ActivityTaskCompleted':
        decisions.append(schedule_activity_task(lastEvent['activityTaskCompletedEventAttributes']['result']))

    swf.respond_decision_task_completed(
      taskToken=newTask['taskToken'],
      decisions=decisions
    )

