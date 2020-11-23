import boto3
import collections
import datetime, json, sys, logging
from pprint import pprint
 
logger = logging.getLogger()
for h in logger.handlers:
  logger.removeHandler(h)

h = logging.StreamHandler(sys.stdout)
FORMAT = ' [%(levelname)s]/%(asctime)s/%(name)s - %(message)s'
h.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(h)
logger.setLevel(logging.INFO)

ec = boto3.client('ec2')

instance_list = []
def lambda_handler(event, context):
  try:
    
    reservations = ec.describe_instances(
        Filters=[
            {'Name': 'tag-key', 'Values': ['automatic-snapshot', 'Backup']},
        ]
    ).get(
        'Reservations', []
    )
 
    instances = sum(
        [
            [i for i in r['Instances']]
            for r in reservations
        ], [])
 
    print("Found {0} instances that need backing up".format(len(instances)))
 
    to_tag = collections.defaultdict(list)
    for instance in instances:
        pprint(instance)
        try:
            retention_days = [
                int(t.get('Value')) for t in instance['Tags']
                if t['Key'] == 'Retention'][0]
        except IndexError:
            retention_days = 7
 
        for dev in instance['BlockDeviceMappings']:
            if dev.get('Ebs', None) is None:
                continue
            vol_id = dev['Ebs']['VolumeId']
            print("Found EBS volume {0} on instance {1}".format(
                vol_id, instance['InstanceId']))
 
            snap = ec.create_snapshot(
                VolumeId=vol_id,
            )
            
            to_tag[retention_days].append(snap['SnapshotId'])
            
            for tags in instance['Tags']:
                if tags["Key"] == 'Name':
                    instancename = tags["Value"]
                    instance_list.append(instancename)
                    
            print("Retaining snapshot {0} of volume {1} from instance {2} for {3} days for {4}".format(
                snap['SnapshotId'],
                vol_id,
                instance['InstanceId'],
                retention_days,
                instancename            ))
            delete_date = datetime.date.today() + datetime.timedelta(days=retention_days)
            delete_fmt = delete_date.strftime('%Y-%m-%d')
            print("Will delete {0} snapshots on {1}".format(len(to_tag[retention_days]), delete_fmt))
            print("instance id now ")
            ec.create_tags( Resources=[snap['SnapshotId']],Tags=[
                            {'Key': 'automatic-ebs-snapshot-delete-on', 'Value': delete_fmt},
                            {'Key': 'Name', 'Value': instancename},
                            {'Key': 'Instance ID', 'Value': instance['InstanceId']}
            ])

    sns = boto3.client('sns')
    try:
      response = sns.publish(
        TopicArn = 'arn:aws:sns:us-east-2:510548384854:automatic-snapshot-status',
        Message ="Hello Team,"+"\n\n"+ "The total number of instances found to perform the backup are {0} instances for Backup".format(len(instances)) + "\n\n" + "The list of instances includes {0}".format(instance_list)+ "\n\n" + "Thanks and best regards"+ "\n" + "PowerX",
        Subject = 'EC2 Backup Status'
      )
      logger.info('SUCCESS: Pushed AMI Baker Results to SNS Topic')
      return "Successfully pushed to Notification to SNS Topic"
    except KeyError as e:
      logger.error('ERROR: Unable to push to SNS Topic: Check [1] SNS Topic ARN is invalid, [2] IAM Role Permissions{0}'.format( str(e) ) )
      logger.error('ERROR: {0}'.format( str(e) ) )
      
  except:
      sns = boto3.client('sns')
      response = sns.publish(
		 TopicArn =  'arn:aws:sns:us-east-2:510548384854:automatic-snapshot-status',
		 Message = 'Failed to create Snapshot'+ "\n" + "InstanceId "+" "+ instancename,
		 Subject = 'EC2 Backup Status'
		 )
		
	  