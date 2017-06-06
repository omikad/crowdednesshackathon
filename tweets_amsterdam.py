import argparse
import csv
import json
import yaml
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

class MyTwitterListener(StreamListener):
    def __init__(self, csv_writer):
        self.csv_writer = csv_writer

    def on_data(self, data):
        js = json.loads(data)
        coordinates = js.get('coordinates')
        if coordinates is not None:
            point = coordinates.get('coordinates')
            if point is not None:
                id_str = js.get('id_str')
                created_at = js.get('created_at')
                lat = point[1]
                lng = point[0]
                text = js.get('text').encode('UTF-8')
                row = [created_at, id_str, 'twitter', lat, lng, text]
                csv_writer.writerow(row)
                print(row)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--kind')
        parser.add_argument('--output')
        args = parser.parse_args()

        csv_file = open(args.output, 'awb', 0)
        csv_writer = csv.writer(csv_file, delimiter=',')

        config = yaml.safe_load(open('../social_keys_config.yml'))

        auth = OAuthHandler(config['twitter_consumer_key'], config['twitter_consumer_secret'])
        auth.set_access_token(config['twitter_access_token'], config['twitter_access_token_secret'])
        stream = Stream(auth = auth, listener = MyTwitterListener(csv_writer))

        if args.kind == 'twitter_geo':
            stream.filter(locations=[4.7685, 52.3216, 5.0173, 52.4251])

        elif args.kind == 'twitter_city':
            stream.filter(track=['amsterdam'])
    except KeyboardInterrupt:
        csv_file.close()
        print('File {} saved'.format(args.output))