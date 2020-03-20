import datetime
import uuid
import json
import pprint
import random
import os

def simulate_tweets(
    user_id, 
    user_name, 
    user_screen_name,
    num_batches, 
    num_deleted_tweets,
    batch_dir,
    min_num_new_non_deleted_tweets_per_batch=1,
    max_num_new_non_deleted_tweets_per_batch=3,
):
    '''
        Creates a number of batches of JSON data in a specified directory.
        The format of the data is as follows: 
        {
            'created_at': <created_at>,
            'id': <tweet id>,
            'text': <tweet text>,
            'user': {
                'id': <user id>,
                'name': <user name>,
                'screen_name': <user screen name>
            }
        }
        Every non-deleted tweet generated is guaranteed to appear in every batch
        after the first in which it is found (ordered by batch number).
        Every deleted tweet is guaranteed to be present in every batch except some
        batch j <= num_batches, and is missing from every batch > j (if any such batch exists).
        The text of each tweet indicates either
            'not deleted X / <# non deleted tweets>'
            OR
            'deleted Y / <# deleted tweets>'
        This makes it easy to see whether our solution catches all the deleted tweets and
        doesn't have false positives for not deleted tweets.

        See the bottom of this file for the call used to generate data. You can tweak 
        it to your liking.

        Params
        user_id: backend id of twitter user
        user_name: human readable name of twitter user (with spaces)
        user_screen_name: @ handle e.g. BillMaher
        num_batches: how many batches to create. 
        num_deleted_tweets: how many tweets to include in some batch, and then
                            exclude in some later batch, and all following batches
        batch_dir: where to put the batch files
        min_num_new_non_deleted_tweets_per_batch: lower bound for how many new tweets
            that are not deleted are introduced in a batch. this is used in a random num generator
            per batch
        max_num_new_non_deleted_tweets_per_batch: upper bound, similar to above

    '''
    # this must be true because with less than 2 batches, a tweet could not have been deleted
    assert num_batches >= 2

    # determine where each deleted tweet appears originally and is deleted
    deleted_tweet_batch_appeared_in = sorted([
        random.randint(0, num_batches - 2)
        for _ in range(num_deleted_tweets)
    ])
    deleted_tweet_batch_deleted_from = [
        random.randint(appeared_in + 1, num_batches - 1)
        for appeared_in in deleted_tweet_batch_appeared_in
    ]
    deleted_tweet_data = list(
        {
            'appeared_in': appeared_in,
            'deleted_from': deleted_from
        }
        for appeared_in, deleted_from in 
        zip(deleted_tweet_batch_appeared_in, deleted_tweet_batch_deleted_from)
    )
    deleted_tweet_seq_num = 1
    non_deleted_tweet_seq_num = 1

    # determine how many non-deleted tweets to add in batch i (we add deleted tweets later)
    num_new_non_deleted_per_batch = [
        random.randint(
            min_num_new_non_deleted_tweets_per_batch, 
            max_num_new_non_deleted_tweets_per_batch
        )
        for _ in range(num_batches)
    ]
    num_non_deleted_tweets = sum(num_new_non_deleted_per_batch)
    batches = []

    # create batches with non-deleted tweets
    for n in num_new_non_deleted_per_batch:
        batch = []
        if len(batches) > 0:
            batch += batches[-1]
        for seq in range(non_deleted_tweet_seq_num, non_deleted_tweet_seq_num + n):
            batch.append(
                create_tweet_dict(
                    str(datetime.datetime.now()),
                    str(uuid.uuid4()),
                    user_id,
                    user_name,
                    user_screen_name,
                    False,
                    seq,
                    num_non_deleted_tweets
                )
            )
        non_deleted_tweet_seq_num += n
        batches.append(batch)

    # insert deleted tweets in the range of batches specified by deleted_tweet_data
    for dtd in deleted_tweet_data:
        appeared_in = dtd['appeared_in']
        deleted_from = dtd['deleted_from']
        for batch_idx in range(appeared_in, deleted_from):
            batches[batch_idx].insert(
                random.randint(0, len(batches[batch_idx])),
                create_tweet_dict(
                    str(datetime.datetime.now()),
                    str(uuid.uuid4()),
                    user_id,
                    user_name,
                    user_screen_name,
                    True,
                    deleted_tweet_seq_num,
                    num_deleted_tweets                
                )
            )
        deleted_tweet_seq_num += 1

    # write to output dir, one file per batch
    for i in range(num_batches):
        with open(os.path.join(batch_dir, f'batch{i+1}.json'), 'w') as batch_f:
            # turn list of dicts into JSON array of objects
            batch_f.write(create_tweet_json(batches[i]))

def create_tweet_dict(
    created_at, 
    tweet_id,
    user_id, 
    user_name, 
    user_screen_name,
    is_deleted, 
    seq_num, 
    seq_len
):
    text_prefix = 'deleted' if is_deleted else 'not deleted'
    text = f'{text_prefix} {seq_num} / {seq_len}'
    tweet_dict = {
        'created_at': created_at,
        'id': tweet_id,
        'text': text,
        'user': {
            'id': user_id, 
            'name': user_name,
            'screen_name': user_screen_name
        }
    }
    return tweet_dict

def create_tweet_json(tweet_dict):
    return json.dumps(tweet_dict, indent=4)


simulate_tweets(
    user_id=str(uuid.uuid4()), # generate arbitrary string using system library 
    user_name='Jim Bob',
    user_screen_name='JimBob',
    num_batches=4,
    num_deleted_tweets=3,
    batch_dir='./batches'
)
