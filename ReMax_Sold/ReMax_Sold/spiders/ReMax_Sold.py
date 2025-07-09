########        ReMax_Sold

import re
import csv
import copy
import scrapy
import pymongo
import mimetypes
from parsel import Selector
from datetime import datetime
import requests,json,time,threading,queue,os
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import NoCredentialsError, ClientError

class ReMax_Sold(scrapy.Spider):
    name = 'ReMax_Sold'
    prefix = 'https://www.propertyspecialists.com.au'
    url = "https://www.propertyspecialists.com.au/sold/"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': 'PHPSESSID=gfak2aestfuaj70m99doeu4ddq; _gid=GA1.3.890489838.1711104146; _gat_gtag_UA_162038751_1=1; _gat_gtag_UA_162038751_1a=1; _ga_CWR9Z60WNN=GS1.1.1711104147.1.1.1711106161.0.0.0; _ga=GA1.1.392527484.1711104146; arp_scroll_position=2369; PHPSESSID=gfak2aestfuaj70m99doeu4ddq',
        # 'Referer': 'https://www.propertyspecialists.com.au/sold/5/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    count = 1
    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for property in response.xpath("//*[contains(@class,'image sold')]"):
            property_url = property.css('a ::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        '''    PAGINATION    '''
        if response.css('.next'):
            next_page_url = self.prefix + response.css('.next ::attr(href)').get('').strip()
            yield scrapy.Request(url=next_page_url, headers=self.headers, callback=self.parse)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1111'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')   # date_data_scanned

            address = response.css('.address ::text').getall()
            property_address = ', '.join(element.strip() for element in address)   # full_adddress
            item['Field5'] = ', '.join(element.strip() for element in address)   # full_adddress

            bed = response.css('.bedrooms span:nth-child(1) ::text').get('')
            if bed:
                item['Field6'] = bed.strip()   # bedrooms
            bath = response.css('.bathrooms span:nth-child(1) ::text').get('')
            if bath:
                item['Field7'] = bath.strip()   # bedrooms
            car = response.css('.carspaces span:nth-child(1) ::text').get('')
            if car:
                item['Field8'] = car.strip()   # bedrooms

            sold_price = response.css('.price .value ::text').get('').strip()   # sold_price
            match = re.search(r'\d{1,3}(?:,\d{3})*', sold_price)
            if match:
                price = int(match.group().replace(',', ''))
                item['Field9'] = price   # sold_price

            description = response.css('#realty_widget_property_description p::text').getall()   # description
            item['Field12'] = ', '.join(element.strip() for element in description)   # description
            '''         Uploading Images on Wasabi S3 Bucket            '''
            new_img_urls = response.css('a.image ::attr(href)').getall()
            Images = ','.join(new_img_urls)
            new_name = property_address.replace(' ','').replace('/','_')
            id = (response.url).split('/')[-2]
            images = self.uploaded(Images,new_name,id)       ## ','saperated Images(string), Property_address, URL_id
            total_images=  ", ".join(images)
            item['Field13'] = total_images   # listing_images

            item['Field14'] = response.url   # external_link

            agent1_name = response.css('.agent-0 .name a ::text').get('').strip()
            if agent1_name:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()   # agent_first_name_1
                item['Field16'] = last_name1.strip()   # agent_surname_name_1
                item['Field17'] = agent1_name.strip()   # agent_full_name_1
                item['Field19'] = response.css('.agent-0 .contact a ::text').get('').strip()     # agent_email_1
                item['Field20'] = response.xpath("//*[contains(@class,'fa fa-mobile')][1]/parent::p/text()").get('').strip()   # agent_mobile_1
                item['Field21'] = response.xpath("//*[contains(@class,'fa fa-phone')][1]/parent::p/text()").get('').strip()   # agent_phnumber_1
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Images = ','.join(response.css('.agent-0 img ::attr(src)').getall())
                new_name = agent1_name.replace(' ','')
                agent1_id = response.css('.agent-0 .name a ::attr(href)').get('').strip()
                id = ''.join(re.findall(r'\d+', agent1_id))
                images = self.uploaded(Agent_Images, new_name, id)        ## ','saperated Images(string), agent_name, agent_id
                agent_image =  ", ".join(images)
                item['Field22'] = agent_image   # listing_images

            agent2 = response.xpath("//*[contains(@class,'agent agent-1 left s-cf')]")
            if agent2:
                item['Field23'] = agent2.css('.name a ::text').get('').strip()   # agent_full_name_2
                item['Field25'] = agent2.css('.contact a ::text').get('').strip()      # agent_email_2
                item['Field26'] = agent2.xpath(".//*[contains(@class,'fa fa-mobile')]/parent::p/text()").get('').strip()   # agent_mobile_2
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Images = ','.join(agent2.css('img ::attr(src)').getall())
                new_name = (agent2.css('.name a ::text').get('').strip()).replace(' ','')
                agent2_id = agent2.css('.name a ::attr(href)').get('').strip()
                id = ''.join(re.findall(r'\d+', agent2_id))
                images = self.uploaded(Agent_Images, new_name, id)        ## ','saperated Images(string), agent_name, agent_id
                item['Field27'] = ", ".join(images)

            agent3 = response.xpath("//*[contains(@class,'agent agent-2 left s-cf')]")
            if agent3:
                item['Field28'] = agent3.css('.name a ::text').get('').strip()   # agent_full_name_3
                item['Field30'] = agent3.css('.contact a ::text').get('').strip()      # agent_email_3
                item['Field31'] = agent3.xpath(".//*[contains(@class,'fa fa-mobile')]/parent::p/text()").get('').strip()   # agent_mobile_3
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Images = ','.join(agent3.css('img ::attr(src)').getall())
                new_name = (agent3.css('.name a ::text').get('').strip()).replace(' ','')
                agent3_id = agent3.css('.name a ::attr(href)').get('').strip()
                id = ''.join(re.findall(r'\d+', agent3_id))
                images = self.uploaded(Agent_Images, new_name, id)        ## ','saperated Images(string), agent_name, agent_id
                item['Field32'] = ", ".join(images)


            property_type = response.xpath("//*[contains(text(),'Property Type')]/following-sibling::span/text()").get('').strip()
            if property_type:
                item['Field35'] = property_type.strip()   # property_type
            land_area = response.xpath("//*[contains(text(),'Land Size')]/following-sibling::span/text()").get('').strip()
            if land_area:
                item['Field36'] = land_area.replace(' m2','')   # land_area

            # print(item)
            self.insert_database(item)

        else:
            print('Data already exists')

    def download_image(self, img_url, file_dir, name):
        try:
            response = requests.request(method='GET', url=img_url)
            if response.status_code == 200:
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                with open(os.path.join(file_dir, name), 'wb') as file:
                    file.write(response.content)
                print(f"{name} Image downloaded successfully.")
            else:
                print("Failed to download the image. Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            print("An error occurred while downloading:", e)
        except Exception as e:
            print(e)

    def create_new_bucket(self, bucket_prefix, bucket_number, s3):
        new_bucket_name = f'{bucket_prefix}_{bucket_number}'
        s3.create_bucket(Bucket=new_bucket_name)
        return new_bucket_name

    def uploaded(self, list_of_img, names,id):
        list_images = [url.strip() for url in list_of_img.split(',')]

        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        s3 = boto3.client(
            's3',
            aws_access_key_id=wasabi_access_key,
            aws_secret_access_key=wasabi_secret_key,
            endpoint_url="https://s3.ap-southeast-1.wasabisys.com",
        )
        # bucket_prefix = 'D_sales_properties'
        bucket_prefix = 'D_REMAX_sold'
        bucket_number = 1
        current_bucket_name = f'{bucket_prefix}_{bucket_number}'

        existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        if current_bucket_name in existing_buckets:
            if current_bucket_name != self.create_new_bucket(bucket_prefix, bucket_number, s3):
                print("Bucket already exists. Create Buket and Run...")
                return

        current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
        print(f"Created and using bucket: {current_bucket_name}")
        try:
            object_count = len(s3.list_objects(Bucket=current_bucket_name).get('Contents', []))
        except s3.exceptions.NoSuchBucket:
            current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
            object_count = 0
        wasabi_url = []

        for index, img in enumerate(list_images, start=1):
            image_url = img

            # local_file_path = 'C:/Users/My PC/Desktop/Images/'
            local_file_path = '/img'

            title_name = f'{names}_{id}_{index}.jpg'

            img_url = f'https://s3.ap-southeast-1.wasabisys.com/{current_bucket_name}/{title_name}'
            self.download_image(image_url, local_file_path, title_name)
            if object_count >= 100000000:
                bucket_number += 1
                current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
                object_count = 0

            bucket_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{current_bucket_name}/*"
                    }
                ]
            }

            s3.put_bucket_policy(Bucket=current_bucket_name, Policy=json.dumps(bucket_policy))
            # uploading
            file_path_on_disk = os.path.join(local_file_path, title_name)

            try:
                content_type, _ = mimetypes.guess_type(file_path_on_disk)
                s3.upload_file(
                    file_path_on_disk,
                    current_bucket_name,
                    title_name,
                    ExtraArgs={'ContentType': content_type} if content_type else None
                )
                self.delete_local_image(file_path_on_disk)
            except NoCredentialsError:
                print("Credentials not available")
            except ClientError as e:
                print(f"An error occurred: {e}")
            object_count += 1
            wasabi_url.append(img_url)
        return wasabi_url

    def delete_local_image(self, file_path):
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error deleting local image: {e}")

    def read_data_base(self,profileUrl):
        url=profileUrl
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn['REMAX']
        collection = db['REMAX_Sold']
        # search_query = {"profileUrl": url}
        search_query = {"Field14": url}

        sale_urls_list_of_DB = []
        # all_matching_data = collection.find(search_query)
        all_matching_data = collection.find_one(search_query)

        if all_matching_data:
            print(all_matching_data.get('Field5'))
            return True
        else:
            return False

    def insert_database(self, new_data):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn['REMAX']
        collection = db['REMAX_Sold']

        collection.insert_one(new_data)
        print("Data inserted successfully!")

