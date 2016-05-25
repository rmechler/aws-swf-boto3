#!/usr/bin/python

import swf_helper
import time

print "Listening for Decision Tasks"

while True:

    task, events = swf_helper.poll_for_decision_task_and_events()

    time.sleep(1)

    print("events {} to {}".format(task['previousStartedEventId'] + 1, task['startedEventId']))

    decisions = []
    for event in events:

      if event['eventType'] == 'WorkflowExecutionStarted':
        print "Dispatching task to worker", task['workflowExecution'], task['workflowType']
        decisions.append(swf_helper.schedule_activity_task('A', '1'))
        decisions.append(swf_helper.schedule_activity_task('B', '1000'))

      elif event['eventType'] == 'ActivityTaskCompleted':
        task_list = swf_helper.get_completed_activity_task_list(event)
        decisions.append(swf_helper.schedule_activity_task(task_list, event['activityTaskCompletedEventAttributes']['result']))

    swf_helper.respond_decision_task_completed(task=task, decisions=decisions)

