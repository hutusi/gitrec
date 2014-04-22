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

import 'recsys.pig';

%default NUM_RECS_PER_CHANNEL 20

set default_parallel $DEFAULT_PARALLEL

----------------------------------------------------------------------------------------------------

contrib_signals     =   load '$OUTPUT_PATH/contrib-signals' using PigStorage()
                        as (user: chararray, item: chararray,
                            weight: float, mapped_from_fork: boolean);

interest_signals    =   load '$OUTPUT_PATH/interest-signals' using PigStorage()
                        as (user: chararray, item: chararray,
                            weight: float, mapped_from_fork: boolean);

item_graph          =   load '$OUTPUT_PATH/item-item-graph' using PigStorage()
                        as (item_A: chararray, item_B: chararray,
                            weight: float, raw_weight: float, rank: int);

item_metadata       =   load '$OUTPUT_PATH/item-metadata' using PigStorage() as (
                            item:          chararray,
                            quality_score: float,
                            activity:      float,
                            popularity:    float,
                            num_stars:     int,
                            num_forks:     int,
                            language:      chararray,
                            description:   chararray
                        );

----------------------------------------------------------------------------------------------------

user_contrib_recs   =   recsys__BuildUserItemRecommendations(
                            contrib_signals, item_graph,
                            $NUM_RECS_PER_CHANNEL, 'true'
                        );

user_interest_recs  =   recsys__BuildUserItemRecommendations(
                            interest_signals, item_graph,
                            $NUM_RECS_PER_CHANNEL, 'true'
                        );

---------------------------------------------------------------------------------------------------
-- ensure no repo is recommended in both channels
-- if it is in both, assign it to the channel in which it has a higher rank

user_contrib_recs   =   foreach user_contrib_recs  generate user, item, reason_item, rank, 1 as channel;
user_interest_recs  =   foreach user_interest_recs generate user, item, reason_item, rank, 2 as channel;
all_recs            =   union user_contrib_recs, user_interest_recs;
all_recs            =   foreach (group all_recs by (user, item)) generate
                            flatten(TOP(1, 3, $1))
                            as (user, item, reason_item, rank, channel);

split all_recs into
    user_contrib_recs if channel == 1,
    user_interest_recs otherwise;

-- the default recsys doesn't support passing this extra "mapped_from_fork" variable around
-- so instead of writing the recsys macros,
-- we rejoin this data back in after the recs have been generated

distinct_contrib    =   foreach contrib_signals generate
                            user, item,
                            (mapped_from_fork == true ? -1 : 0) as mapped_from_fork;
distinct_contrib    =   foreach (group distinct_contrib by (user, item))
                            generate flatten(TOP(1, 2, $1))
                                  as (user, item, mapped_from_fork);
user_contrib_recs   =   foreach (join distinct_contrib   by (user, item),
                                      user_contrib_recs  by (user, reason_item)) generate
                            user_contrib_recs::user as user,
                            user_contrib_recs::item as item,
                            reason_item             as reason_item,
                            rank                    as old_rank,
                            -mapped_from_fork       as mapped_from_fork;

distinct_interest   =   foreach interest_signals generate
                            user, item,
                            (mapped_from_fork == true ? -1 : 0) as mapped_from_fork;
distinct_interest   =   foreach (group distinct_interest by (user, item))
                            generate flatten(TOP(1, 2, $1))
                                  as (user, item, mapped_from_fork);
user_interest_recs  =   foreach (join distinct_interest  by (user, item),
                                      user_interest_recs by (user, reason_item)) generate
                            user_interest_recs::user as user,
                            user_interest_recs::item as item,
                            reason_item              as reason_item,
                            rank                     as old_rank,
                            -mapped_from_fork        as mapped_from_fork;

----------------------------------------------------------------------------------------------------

define PostprocessRecs(recs, item_metadata) returns result {
    joined  =   foreach (join $item_metadata by item, $recs by item) generate
                    user                 as user,
                    $item_metadata::item as item,
                    reason_item          as reason_item,
                    old_rank             as old_rank,
                    mapped_from_fork     as mapped_from_fork,
                    num_forks            as num_forks,
                    num_stars            as num_stars,
                    language             as language,
                    description          as description;

    $result =   foreach (group joined by user) {
                    sorted = order $1 by old_rank asc;
                    generate flatten(recsys__Enumerate(sorted)) as
                    (user, item, reason_item, old_rank, mapped_from_fork,
                     num_forks, num_stars, language, description, rank);
                }

    $result =   foreach $result generate
                    user, rank, item, reason_item,
                    mapped_from_fork,
                    num_forks, num_stars, language, description;

};

user_contrib_recs   =   PostprocessRecs(user_contrib_recs,  item_metadata);
user_interest_recs  =   PostprocessRecs(user_interest_recs, item_metadata);

----------------------------------------------------------------------------------------------------

rmf $OUTPUT_PATH/user-contrib-recs;
rmf $OUTPUT_PATH/user-interest-recs;

store user_contrib_recs  into '$OUTPUT_PATH/user-contrib-recs'  using PigStorage();
store user_interest_recs into '$OUTPUT_PATH/user-interest-recs' using PigStorage();