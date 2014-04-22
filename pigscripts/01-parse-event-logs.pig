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
 * distributed under the License is distributed on an "as is" Basis,
 * WITHOUT WARRANTIES or CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------

-- we will be computing an "quality/importance" score for each repo
-- which is set equal to popularity * sqrt(min(activity, $MAX_ACTIVITY_CONSIDERED))
-- where popularity = 2*[# forks] + [# stars]
-- and   activity   = [# pushes] + [# pull requests]

%default MAX_ACTIVITY_CONSIDERED 64

set default_parallel $DEFAULT_PARALLEL

----------------------------------------------------------------------------------------------------
-- select the following types of events from the logs:
-- ForkEvent PullRequestEvent PushEvent WatchEvent
-- note that WatchEvent actually means that the user starred the repo

logs_2013   =   load '$INPUT_PATH_2013'
                using org.apache.pig.piggybank.storage.JsonLoader('
                    actor: chararray,
                    actor_attributes: (
                        gravatar_id: chararray,
                        company: chararray
                    ),
                    repository: (
                        owner: chararray,
                        name: chararray,
                        fork: chararray,
                        language: chararray,
                        description: chararray,
                        stargazers: int,
                        forks: int
                    ),
                    type: chararray,
                    created_at: chararray
                ');

logs_2014   =   load '$INPUT_PATH_2014'
                using org.apache.pig.piggybank.storage.JsonLoader('
                    actor: chararray,
                    actor_attributes: (
                        gravatar_id: chararray,
                        company: chararray
                    ),
                    repository: (
                        owner: chararray,
                        name: chararray,
                        fork: chararray,
                        language: chararray,
                        description: chararray,
                        stargazers: int,
                        forks: int
                    ),
                    type: chararray,
                    created_at: chararray
                ');

logs    =   union logs_2013, logs_2014;

/*
logs    =   load '$INPUT_PATH'
            using org.apache.pig.piggybank.storage.JsonLoader('
                actor: chararray,
                actor_attributes: (
                    gravatar_id: chararray,
                    company: chararray
                ),
                repository: (
                    owner: chararray,
                    name: chararray,
                    fork: chararray,
                    language: chararray,
                    description: chararray,
                    stargazers: int,
                    forks: int
                ),
                type: chararray,
                created_at: chararray
            ');
*/

logs    =   filter logs by (
                (
                    actor            is not null and
                    repository.owner is not null and
                    repository.name  is not null and
                    repository.fork  is not null and
                    type             is not null and
                    created_at       is not null
                )

                and

                (
                    type == 'ForkEvent'        or
                    type == 'PullRequestEvent' or
                    type == 'PushEvent'        or
                    type == 'WatchEvent'
                )

                and

                (
                    -- extremely common "demo" repos that mess with the recsys graph
                    -- so we filter them out
                    repository.name != 'try_git' and
                    repository.name != 'Spoon-Knife'
                )
            );

logs    =   foreach logs generate
                LOWER(actor)           as user,
                LOWER(CONCAT(repository.owner, CONCAT('/', repository.name)))
                                       as item,
                type                   as signal,
                created_at             as timestamp,
                actor_attributes.gravatar_id
                                       as gravatar_id,
                repository.name        as repo_name,
                (repository.fork == 'true' ? true : false)
                                       as is_a_fork,
                repository.forks       as num_forks,
                repository.stargazers  as num_stars,
                (repository.language is null ? 'Unknown' : repository.language)
                                       as language,
                repository.description as description;

logs    =   filter logs by SUBSTRING(item, 0, 1) != '/';

-- for restarting the script after something goes wrong / a change needs to be made
-- without having to reparse all the JSON logs
/*
logs    =   load '$OUTPUT_PATH/parsed-logs' using PigStorage() as (
                user:        chararray,
                item:        chararray,
                signal:      chararray,
                timestamp:   chararray,
                gravatar_id: chararray,
                repo_name:   chararray,
                is_a_fork:   boolean,
                num_forks:   int,
                num_stars:   int,
                language:    chararray,
                description: chararray
            );
*/

----------------------------------------------------------------------------------------------------
-- get the gravatar ids for each user
-- you can get an image from a gravatar id by hitting:
-- http://www.gravatar.com/avatar/[the gravatar id]

gravatar_logs   =   foreach (filter logs by SIZE(gravatar_id) == 32) generate
                        user, timestamp, gravatar_id;
gravatar_ids    =   foreach (group gravatar_logs by user) generate
                        flatten(TOP(1, 1, $1))
                        as (user, timestamp, gravatar_id);
gravatar_ids    =   foreach gravatar_ids generate user, gravatar_id;

----------------------------------------------------------------------------------------------------
-- we have repo metadata with every event,
-- but we only want the metadata for the most recent state of the repo.
-- we also only care about repos that are not forks,
-- since we will map events for forked repos back to the original repo in the next section

metadata_info   =   foreach logs generate
                        item, timestamp,
                        is_a_fork, num_forks, num_stars, language, description;
metadata_info   =   filter metadata_info by not is_a_fork;

item_metadata   =   foreach (group metadata_info by item) generate
                        flatten(TOP(1, 1, $1))
                        as (item, timestamp,
                            is_a_fork, num_forks, num_stars, language, description);

item_metadata   =   foreach item_metadata generate
                        item,
                        2 * num_forks + num_stars
                            as popularity,
                               num_forks,
                               num_stars,
                               language,
                               description;

----------------------------------------------------------------------------------------------------
-- we want to map interactions with forks of repos
-- back to the original repo that the user forked,
-- but the logs don't give this information.
-- so we assume that a the source of a fork
-- is the most-forked repo with the same name and same language.
-- this seems to work well enough in the average case.

fork_info       =   foreach logs generate
                        item, timestamp,
                        repo_name, is_a_fork, num_forks, language;

split fork_info into fork_repos if is_a_fork,
                     source_repos otherwise;

-- most recent (repo_name, repo_lang) for each item
fork_repos      =   foreach (group fork_repos by item) generate
                        flatten(TOP(1, 1, $1))
                        as (item, timestamp, repo_name, is_a_fork, num_forks, language);
fork_repos      =   foreach fork_repos generate item, repo_name, language;

-- item with greatest # forks for each (repo_name, repo_lang) key
source_repos    =   foreach (group source_repos by (repo_name, language)) generate
                        FLATTEN(TOP(1, 4, $1))
                        as (item, timestamp, repo_name, is_a_fork, num_forks, language);
source_repos    =   foreach source_repos generate item, repo_name, language;

fork_map        =   foreach (join source_repos by (repo_name, language),
                                  fork_repos   by (repo_name, language)) generate
                        fork_repos::item   as fork_item,
                        source_repos::item as source_item;

signals         =   foreach (join fork_map by fork_item right outer, logs by item) generate
                        user   as user,
                        (fork_map::source_item is not null ? fork_map::source_item : item)
                               as item,
                        signal as signal,
                        (fork_map::source_item is not null ? true : false) as mapped_from_fork;

----------------------------------------------------------------------------------------------------
-- we will be generating two "tracks" for recommendations
-- 1) recommended based on your contributions (push + pull reqs)
-- 2) recommended based on your interests (stars + fork)
--
-- because these tracks are qualitatively different,
-- we split the signals into these two categories
-- and will be generating two separate item-item graphs for each one
--

split signals into
    contrib_signals  if signal == 'PushEvent' or signal == 'PullRequestEvent',
    interest_signals otherwise;

contrib_signals     =   foreach contrib_signals generate
                            user, item, 1.0 as weight, mapped_from_fork;

interest_signals    =   foreach interest_signals generate
                            user, item,
                            (signal == 'ForkEvent' ? 3.0 : 1.0) as weight,
                            mapped_from_fork;

----------------------------------------------------------------------------------------------------
-- when assigning a "quality/importance score" to each repo,
-- we want to take into account how actively the repo is being developed
-- in addition to it's raw popularity

item_activities =   foreach (group contrib_signals by item) generate
                        group as item,
                        (float) SUM($1.weight) as activity;

item_metadata   =   foreach (join item_activities by item, item_metadata by item) generate
                            item_metadata::item as item,
                            (float) (popularity *
                                SQRT((activity < $MAX_ACTIVITY_CONSIDERED ?
                                      activity : $MAX_ACTIVITY_CONSIDERED)))
                                                as quality_score,
                                       activity as activity,
                                     popularity as popularity,
                                      num_forks as num_forks,
                                      num_stars as num_stars,
                                       language as language,
                                    description as description;

----------------------------------------------------------------------------------------------------

rmf $OUTPUT_PATH/parsed-logs;
rmf $OUTPUT_PATH/contrib-signals;
rmf $OUTPUT_PATH/interest-signals;
rmf $OUTPUT_PATH/item-metadata;
rmf $OUTPUT_PATH/gravatar-ids;
rmf $OUTPUT_PATH/fork-map;

store logs             into '$OUTPUT_PATH/parsed-logs'      using PigStorage();
store contrib_signals  into '$OUTPUT_PATH/contrib-signals'  using PigStorage();
store interest_signals into '$OUTPUT_PATH/interest-signals' using PigStorage();
store item_metadata    into '$OUTPUT_PATH/item-metadata'    using PigStorage();
store gravatar_ids     into '$OUTPUT_PATH/gravatar-ids'     using PigStorage();
store fork_map         into '$OUTPUT_PATH/fork-map'         using PigStorage();