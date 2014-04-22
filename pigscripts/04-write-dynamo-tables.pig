/*
 * Copyright 2013 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------

set dynamodb.throughput.write.percent 1.0;

----------------------------------------------------------------------------------------------------

grav_ids            =   load '$OUTPUT_PATH/gravatar-ids'
                        using PigStorage()
                        as (user: chararray, gravatar: chararray);

grav_ids            =   distinct grav_ids;

item_recs           =   load '$OUTPUT_PATH/item-item-recs'
                        using PigStorage()
                        as (repo_lowered: chararray, rank: int, rec: chararray,
                            lang: chararray, forks: int, stars: int, description: chararray);

user_contrib_recs   =   load '$OUTPUT_PATH/user-contrib-recs'
                        using PigStorage()
                        as (user_lowered: chararray, rank: int, rec: chararray, reason: chararray,
                            reason_flag: int, forks: int, stars: int, lang: chararray, description: chararray);

user_interest_recs  =   load '$OUTPUT_PATH/user-interest-recs'
                        using PigStorage()
                        as (user_lowered: chararray, rank: int, rec: chararray, reason: chararray,
                            reason_flag: int, forks: int, stars: int, lang: chararray, description: chararray);


----------------------------------------------------------------------------------------------------

store grav_ids
 into '$OUTPUT_PATH/unused-gravatar-table-data'
using com.mortardata.pig.storage.DynamoDBStorage('$GRAVATAR_TABLE', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY');

store item_recs
 into '$OUTPUT_PATH/unused-ii-table-data'
using com.mortardata.pig.storage.DynamoDBStorage('$ITEM_ITEM_TABLE', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY');

store user_contrib_recs
 into '$OUTPUT_PATH/unused-user-contrib-table-data'
using com.mortardata.pig.storage.DynamoDBStorage('$USER_CONTRIB_TABLE', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY');

store user_interest_recs
 into '$OUTPUT_PATH/unused-user-interest-table-data'
using com.mortardata.pig.storage.DynamoDBStorage('$USER_INTEREST_TABLE', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY');