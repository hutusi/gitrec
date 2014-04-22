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
import 'recsys_alternatives.pig';

%default SIGNAL_WEIGHTING_LOGISTIC_PARAM 1.00 -- weight 1 => 0.462, weight 3 => 0.905
%default MIN_LINK_WEIGHT                 1.25 -- 3 stars or 1 star + 1 fork
%default MAX_LINKS_PER_USER               100
%default PRIOR                           7.39 -- ~= 16 stars from distinct users
%default NUM_RECS_PER_ITEM                 20

-- for jan-2013 to apr-2014, this corresponds
-- to the mapping of the 99.5% quantile of quality scores => 0.9
%default QUALITY_MULTIPLIER_LOGISTIC_PARAM  0.00227

-- to the mapping of the 99% quantile of quality scores => 0.9
-- (more novel / less popular compared to 99.5% quant, on average seems slightly worse)
--%default QUALITY_MULTIPLIER_LOGISTIC_PARAM  0.00569

set default_parallel $DEFAULT_PARALLEL

----------------------------------------------------------------------------------------------------

interest_signals    =   load '$OUTPUT_PATH/interest-signals' using PigStorage()
                        as (user: chararray, item: chararray, weight: float);

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

item_quality_scores =   foreach item_metadata generate item, quality_score;

----------------------------------------------------------------------------------------------------

-- modification of standard mortar-recsys GetItemItemRecommendations macro

define gitrec__GetItemItemRecommendations(user_item_signals, item_quality_scores)
returns item_item_recs {
    ii_links_raw, item_weights  =   recsys__BuildItemItemGraph(
                                       $user_item_signals,
                                       $SIGNAL_WEIGHTING_LOGISTIC_PARAM,
                                       $MIN_LINK_WEIGHT,
                                       $MAX_LINKS_PER_USER
                                    );

    -- similar to recsys__AdjustItemItemGraphWeight_withPopularityBoost,
    -- but we use the scores based on popularity and activity we calculated in 01-generate-signals.pig
    -- instead of the item weights from recsys__BuildItemItemGraph

    item_weights    =   foreach (join $item_quality_scores by item, item_weights by item) generate
                            item_weights::item as item,
                            overall_weight     as overall_weight,
                            recsys_udfs.logistic_scale(quality_score, $QUALITY_MULTIPLIER_LOGISTIC_PARAM)
                                               as quality_multiplier;

    ii_links        =   foreach (join item_weights by item, ii_links_raw by item_B) generate
                            item_A as item_A,
                            item_B as item_B,
                            (float) ((weight * quality_multiplier) / (overall_weight + $PRIOR))
                            as weight,
                            weight as raw_weight;

    $item_item_recs =   recsys__BuildItemItemRecommendationsFromGraph_skipShortestPaths(
                           ii_links, $NUM_RECS_PER_ITEM
                        );
};

----------------------------------------------------------------------------------------------------

item_item_graph =   gitrec__GetItemItemRecommendations(interest_signals, item_quality_scores);

-- apply a diversity adjustment to penalize multiple recs from the same owner

item_item_graph =   foreach item_item_graph generate
                        item_A, item_B, weight, raw_weight,
                        REGEX_EXTRACT(item_B, '(.*)/.*', 1) as item_B_owner;

item_item_graph =   foreach (group item_item_graph by (item_A, item_B_owner)) {
                        ordered = order $1 by weight desc;
                        generate flatten(recsys__Enumerate(ordered))
                              as (item_A, item_B, weight, raw_weight, item_B_owner, owner_rank);
                    }

item_item_graph =   foreach item_item_graph generate
                        item_A, item_B,
                        (weight / owner_rank) as weight,
                        raw_weight;

item_item_graph =   foreach (group item_item_graph by item_A) {
                        ordered = order $1 by weight desc;
                        generate flatten(recsys__Enumerate(ordered))
                              as (item_A, item_B, weight, raw_weight, rank);
                    }

----------------------------------------------------------------------------------------------------

item_item_recs  =   foreach (join item_metadata by item, item_item_graph by item_B) generate
                             item_A as item,
                               rank as rank,
                             item_B as rec,
                           language as language,
                          num_forks as num_forks,
                          num_stars as num_stars,
                        description as description;

item_item_recs  =   foreach (group item_item_recs by item) {
                        ordered = order item_item_recs by rank asc;
                        generate flatten(ordered);
                    }

----------------------------------------------------------------------------------------------------

rmf $OUTPUT_PATH/item-item-graph;
rmf $OUTPUT_PATH/item-item-recs;

store item_item_graph into '$OUTPUT_PATH/item-item-graph' using PigStorage();
store item_item_recs  into '$OUTPUT_PATH/item-item-recs'  using PigStorage();