import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask
from boto.dynamodb2.types import NUMBER, STRING

from mortar.luigi import mortartask
from mortar.luigi import dynamodb


# helper function
def create_full_path(base_path, sub_path):
    return '%s/%s' % (base_path, sub_path)

# REPLACE WITH YOUR PROJECT NAME
MORTAR_PROJECT = 'XXX Fill me in'

'''
 Example run command:
    mortar local:luigi luigiscripts/gitrec-luigi.py -p output-base-path=s3://mortar-prod-sandbox/github
        -p input-base-path=s3://mortar-example-data/github -p date-string=20140422
'''

class GitRecPigscriptTask(mortartask.MortarProjectPigscriptTask):
    # s3 path to the folder where the input data is located
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # cluster size to use
    cluster_size = luigi.IntParameter(default=20)

    # use spot instances to lower cost
    use_spot_instances = luigi.Parameter(True)

    # pig version
    pig_version = '0.12'

    def project(self):
        """
        Name of the mortar project to run.
        """
        return MORTAR_PROJECT

    def token_path(self):
        return self.output_base_path

    def default_parallel(self):
        return (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE


class ParseEventLogs(GitRecPigscriptTask):

    def requires(self):
        return [S3PathTask(create_full_path(self.input_base_path, 'raw_events'))]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'parsed-logs')),
                S3Target(create_full_path(self.output_base_path, 'contrib-signals')),
                S3Target(create_full_path(self.output_base_path, 'interest-signals')),
                S3Target(create_full_path(self.output_base_path, 'item-metadata')),
                S3Target(create_full_path(self.output_base_path, 'gravatar-ids')),
                S3Target(create_full_path(self.output_base_path, 'fork-map'))]

    def parameters(self):
        return {'INPUT_PATH_2013': create_full_path(self.input_base_path, 'raw_events/2013/12/01/*'),
                'INPUT_PATH_2014': create_full_path(self.input_base_path, 'raw_events/2014/01/01/*'),
                'OUTPUT_PATH': self.output_base_path,
                'DEFAULT_PARALLEL': self.default_parallel()}

    def script(self):
        """
        Name of the script to run.
        """
        return '01-parse-event-logs'


class GenItemGraph(GitRecPigscriptTask):

    def requires(self):
        return [ParseEventLogs(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'item-item-graph')),
                S3Target(create_full_path(self.output_base_path, 'item-item-recs'))]

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'DEFAULT_PARALLEL': self.default_parallel()}

    def script(self):
        """
        Name of the script to run.
        """
        return '02-gen-item-graph'

class GenUserRecChannels(GitRecPigscriptTask):

    def requires(self):
        return [GenItemGraph(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user-contrib-recs')),
                S3Target(create_full_path(self.output_base_path, 'user-interest-recs'))]

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'DEFAULT_PARALLEL': self.default_parallel()}

    def script(self):
        """
        Name of the script to run.
        """
        return '03-gen-user-rec-channels'

class CreateRepoRecsTable(dynamodb.CreateDynamoDBTable):
    """
    Creates a DynamoDB table for storing the item-item recommendations
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # initial read throughput of the table
    read_throughput = luigi.IntParameter(1)

    # initial write throughput of the table
    write_throughput = luigi.IntParameter(3000)

    # identifying date string
    date_string = luigi.Parameter()

    # primary hash key for the DynamoDB table
    hash_key = 'repo_lowered'

    # type of the hash key
    hash_key_type = STRING

    # range key for the DynamoDB table
    range_key = 'rank'

    # type of the range key
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # append '-II' to distinguish between this and the user-item table
    def table_name(self):
        return 'github_sqrt_repo_recs_%s' % self.date_string

    def requires(self):
        return [GenUserRecChannels(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]

class CreateContribRecsTable(dynamodb.CreateDynamoDBTable):
    """
    Creates a DynamoDB table for storing the item-item recommendations
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # initial read throughput of the table
    read_throughput = luigi.IntParameter(1)

    # initial write throughput of the table
    write_throughput = luigi.IntParameter(3000)

    # identifying date string
    date_string = luigi.Parameter()

    # primary hash key for the DynamoDB table
    hash_key = 'user_lowered'

    # type of the hash key
    hash_key_type = STRING

    # range key for the DynamoDB table
    range_key = 'rank'

    # type of the range key
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def table_name(self):
        return 'github_sqrt_contrib_recs_%s' % self.date_string

    def requires(self):
        return [CreateRepoRecsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

class CreateGravatarIdsTable(dynamodb.CreateDynamoDBTable):


    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # initial read throughput of the table
    read_throughput = luigi.IntParameter(1)

    # initial write throughput of the table
    write_throughput = luigi.IntParameter(30000)

    # identifying date string
    date_string = luigi.Parameter()

    # primary hash key for the DynamoDB table
    hash_key = 'user'

    # type of the hash key
    hash_key_type = STRING

    # range key for the DynamoDB table
    range_key = None

    # type of the range key
    range_key_type = None


    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def table_name(self):
        return 'github_sqrt_user_gravatar_ids_%s' % self.date_string

    def requires(self):
        return [CreateContribRecsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

class CreateUserInterestRecsTable(dynamodb.CreateDynamoDBTable):
    """
    Creates a DynamoDB table for storing the item-item recommendations
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # initial read throughput of the table
    read_throughput = luigi.IntParameter(1)

    # initial write throughput of the table
    write_throughput = luigi.IntParameter(3000)

    # identifying date string
    date_string = luigi.Parameter()

    # primary hash key for the DynamoDB table
    hash_key = 'user_lowered'

    # type of the hash key
    hash_key_type = STRING

    # range key for the DynamoDB table
    range_key = 'rank'

    # type of the range key
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def table_name(self):
        return 'github_user_interest_recs_%s' % self.date_string

    def requires(self):
        return [CreateGravatarIdsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]


class WriteDynamoDBTables(GitRecPigscriptTask):

    def requires(self):
        return [CreateContribRecsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string),
                CreateRepoRecsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string),
                CreateGravatarIdsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string),
                CreateUserInterestRecsTable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

    def script_output(self):
        return []

    # identifying date string
    date_string = luigi.Parameter()

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'USER_CONTRIB_TABLE': 'github_sqrt_contrib_recs_%s' % self.date_string,
                'GRAVATAR_TABLE': 'github_sqrt_user_gravatar_ids_%s' % self.date_string,
                'USER_INTEREST_TABLE': 'github_user_interest_recs_%s' % self.date_string,
                'ITEM_ITEM_TABLE': 'github_sqrt_repo_recs_%s' % self.date_string,
                'AWS_ACCESS_KEY_ID': configuration.get_config().get('dynamodb', 'aws_access_key_id'),
                'AWS_SECRET_ACCESS_KEY': configuration.get_config().get('dynamodb', 'aws_secret_access_key')}

    def script(self):
        """
        Name of the script to run.
        """
        return '04-write-dynamo-tables'

class UpdateRepoRecsThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    After writing to the table, ramp down the writes and/or up the writes to make the table
    ready for production.
    """

    # target read throughput of the dynamodb table
    read_throughput = luigi.IntParameter(8)

    # target write throughput of the dynamodb table
    write_throughput = luigi.IntParameter(1)

    # identifying date string
    date_string = luigi.Parameter()

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    def requires(self):
        return [WriteDynamoDBTables(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

    def table_name(self):
        return 'github_sqrt_repo_recs_%s' % self.date_string

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

class UpdateContribRecsThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    After writing to the table, ramp down the writes and/or up the writes to make the table
    ready for production.
    """

    # target read throughput of the dynamodb table
    read_throughput = luigi.IntParameter(8)

    # target write throughput of the dynamodb table
    write_throughput = luigi.IntParameter(1)

    # identifying date string
    date_string = luigi.Parameter()

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    def requires(self):
        return [UpdateRepoRecsThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

    def table_name(self):
        return 'github_sqrt_contrib_recs_%s' % self.date_string

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))


class UpdateGravatarIdsThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    After writing to the table, ramp down the writes and/or up the writes to make the table
    ready for production.
    """

    # target read throughput of the dynamodb table
    read_throughput = luigi.IntParameter(8)

    # target write throughput of the dynamodb table
    write_throughput = luigi.IntParameter(1)

    # identifying date string
    date_string = luigi.Parameter()

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    def requires(self):
        return [UpdateContribRecsThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

    def table_name(self):
        return 'github_sqrt_user_gravatar_ids_%s' % self.date_string

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

class UpdateUserInterestThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    After writing to the table, ramp down the writes and/or up the writes to make the table
    ready for production.
    """

    # target read throughput of the dynamodb table
    read_throughput = luigi.IntParameter(8)

    # target write throughput of the dynamodb table
    write_throughput = luigi.IntParameter(1)

    # identifying date string
    date_string = luigi.Parameter()

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    def requires(self):
        return [UpdateGravatarIdsThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

    def table_name(self):
        return  'github_user_interest_recs_%s' % self.date_string

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    When the pipeline is completed, shut down all active clusters not currently running jobs
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # identifying date string
    date_string = luigi.Parameter()

    def requires(self):
        return [UpdateRepoRecsThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string),
                UpdateContribRecsThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string),
                UpdateGravatarIdsThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string),
                UpdateUserInterestThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, date_string=self.date_string)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]

if __name__ == "__main__":
    luigi.run(main_task_cls=ShutdownClusters)
