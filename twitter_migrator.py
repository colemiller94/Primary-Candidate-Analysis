from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler

import credentials

class TwitterStreamer():

	def stream_tweets(self,filename,tag_list,user_list):

		listener = StdOutListener(filename)
		auth = OAuthHandler(credentials.CONSUMER_KEY,credentials.CONSUMER_SECRET)
		auth.set_access_token(credentials.ACCESS_TOKEN,credentials.ACCESS_TOKEN_SECRET)

		stream = Stream(auth,listener)
		stream.filter(track=tag_list,follow=user_list)

class StdOutListener(StreamListener):

	def __init__(self,filename):
		self.filename=filename
	def on_data(self,data):
		try:
			print(data)
			with open(self.filename,'a') as fn:
				fn.write(data)
			return True
		except BaseException as e:
			print(f'Error on_data: {e}')
	def on_error(self,status):
		print(status)

if __name__ == '__main__':

	tag_list = ['Joe Biden','Bernie Sanders','Kamala Harris', 'Cory Booker',
	'Elizabeth Warren',"Beto O'Rourke","Beto ORourke",'Eric Holder','Sherrod Brown',
	'Amy Klobuchar','Michael Bloomberg','John Hickenlooper','Kirsten Gillibrand',
	'Andrew Yang','Julian Castro','Julián Castro','Eric Swalwell','Tulsi Gabbard',
	'Jay Inslee','Pete Buttigieg', 'John Delaney','Mike Gravel','Wayne Messam',
	'Tim Ryan','Marianne Willamson','Stacy Abrams','Mayor Pete']
	user_list = ['JoeBiden','BernieSanders','KamalaHarris','CoryBooker',
	'ewarren','BetoORourke','EricHolder','SherrodBrown','amyklobuchar',
	'MikeBloomberg','Hickenlooper','SenGillibrand','AndrewYang','JulianCastro',
	'ericswalwell','TulsiGabbard','JayInslee','PeteButtigieg','JohnDelaney',
	'MikeGravel','WayneMessam','TimRyan','marwilliamson','staceyabrams']
	user_ids = ['939091','216776631','30354991','15808765','357606935','342863309','3333055535',
	'24768753','33537967','16581604','117839957','72198806','2228878592','19682187','377609596',
	'26637348','21789463','226222147','426028646','14709326','33954145','466532637','21522338',
	'216065430']
	filename = 'tweets.json'

	streamer = TwitterStreamer()
	streamer.stream_tweets(filename,tag_list,user_ids)
