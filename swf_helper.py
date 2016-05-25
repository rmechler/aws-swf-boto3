import boto3
from botocore.client import Config
import uuid
import logging

logging.basicConfig()

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

# read_timeout should be at least 10 seconds longer than poll timeout (which is 60 seconds)
swf = boto3.client('swf', config=Config(connect_timeout=50, read_timeout=70))

DOMAIN = "rmechler_test"
WORKFLOW = "rmechler_test_workflow"
TASKNAME = "rmechler_test_task"
VERSION = "0.1"
TASKLIST = "rmechler_test_tasklist"


def start_workflow(workflow_id, input_data='', workflowTimeout=3600, deciderTimeout=30):
    """Start workflow execution.
    """
    return swf.start_workflow_execution(domain=DOMAIN,
                                        workflowId=workflow_id,
                                        workflowType={'name': WORKFLOW, 'version': VERSION},
                                        taskList={'name': TASKLIST},
                                        executionStartToCloseTimeout=str(workflowTimeout),
                                        taskStartToCloseTimeout=str(deciderTimeout),
                                        input=input_data)


def schedule_activity_task(task_list, input_data):

    LOGGER.debug("ScheduleActivityTask: {}".format(input_data))
    return {
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': {
                'activityType':{
                    'name': TASKNAME,
                    'version': VERSION
                    },
                'activityId': 'activityid-' + str(uuid.uuid4()),
                'input': input_data,
                'scheduleToCloseTimeout': 'NONE',
                'scheduleToStartTimeout': 'NONE',
                'startToCloseTimeout': 'NONE',
                'heartbeatTimeout': 'NONE',
                'taskList': {'name': task_list},
            }
          }


def poll_for_decision_task_and_events(identity=''):
    """Poll for decision task and latest decision events.
    """
    while True:
        task = swf.poll_for_decision_task(domain=DOMAIN,
                                          taskList={'name': TASKLIST},
                                          identity=identity, # TODO: what to use as identity?
                                          reverseOrder=True)

        if 'taskToken' not in task:
            LOGGER.info("Poll timed out, no new task.  Repoll")

        elif 'events' in task:
            # TODO: need to handle paged results?

            # put all non-decision events in a dictionary
            events = {e['eventId']: e for e in task['events'] if not e['eventType'].startswith('Decision')}

            # get just the new events for returning to caller
            new_events = [e for e in events.values() if (e['eventId'] <= task['startedEventId']
                                                         and e['eventId'] > task['previousStartedEventId'])]

            # Inject the actual scheduled event ID so we can get things like the task list name.
            for event in new_events:
                if event['eventType'] == 'ActivityTaskCompleted':
                    # TODO make sure we have the scheduled event first
                    event['activityTaskCompletedEventAttributes']['scheduledEvent'] = events[event['activityTaskCompletedEventAttributes']['scheduledEventId']]

            return task, new_events


def get_completed_activity_task_list(event):
    """Get task list for completed activity task event.
    """
    try:
        return event['activityTaskCompletedEventAttributes']['scheduledEvent']['activityTaskScheduledEventAttributes']['taskList']['name']
    except:
        return None


def respond_decision_task_completed(task, decisions):
    """Respond with decisions.
    """
    swf.respond_decision_task_completed(taskToken=task['taskToken'], decisions=decisions)


def poll_for_activity_task(task_list, identity=''):
    """Poll for activity task.
    """
    while True:
        task = swf.poll_for_activity_task(domain=DOMAIN, taskList={'name': task_list}, identity=identity)

        if 'taskToken' not in task:
            LOGGER.info("Poll timed out, no new task.  Repoll")

        else:
            return task


def respond_activity_task_completed(task, result=''):
    """Complete activity task.
    """
    swf.respond_activity_task_completed(taskToken=task['taskToken'], result=result)
