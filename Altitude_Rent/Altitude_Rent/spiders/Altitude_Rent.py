#################       Altitude_Rent

from copy import deepcopy
import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from scrapy import Selector

class Altitude_Rent(scrapy.Spider):
    name = 'Altitude_Rent'
    prefix = 'https://www.altituderealestate.com.au'
    url = ("https://www.altituderealestate.com.au/data/results/?listing_category=residential&listing_sale_method=lease&"
           "sort=date-desc&pg={}")
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        # 'cookie': '_ga=GA1.3.1124291693.1713706627; _gid=GA1.3.578464085.1713874628; _WHEELS_AUTHENTICITY=ULaO7W4MM5kryr%2BzTPlA1xv94xiXRcbsGfrbghRF6crbyqSmtV3iSi7eIO6rjynX4Emr9xnXoAtOlQ7aCCnjPtUpL6zgsNc4GPuDWBD4zW%2FFN8INBWdf643RRJvqGkVrluh4XmPzOTxTPsAv6cYLcA%3D%3D; FLASH=%7B%7D; _gat=1; _ga_5RE5SNFZ1T=GS1.3.1713956170.3.1.1713956201.29.0.0; arp_scroll_position=1929; FLASH=%7B%7D',
        'priority': 'u=0, i',
        # 'referer': 'https://www.altituderealestate.com.au/sold',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    count = 1
    db = 'Altitude'
    collection = 'Altitude_Rent'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        yield scrapy.Request(url='https://www.altituderealestate.com.au/residential/lease', callback=self.parse, headers=self.headers)

    def parse(self, response):
        total_entries = int(response.css('.heading-div em::text').get('').strip())
        page_no = 1
        entry = 0
        print('Total Entries are : ', total_entries)
        yield scrapy.Request(url=self.url.format(page_no), callback=self.parse_listing, headers=self.headers,
                                 meta={'page_no':page_no,'entry':entry,'total_entries':total_entries})

    def parse_listing(self, response):
        entry = response.meta['entry']
        data = json.loads(response.text)
        html_string = data
        selector = Selector(text=html_string.replace('\n', '').replace('\t', ''))
        for property in selector.css('a'):
            entry += 1
            property_url = property.css('::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield response.follow(url=property_url, headers=self.headers, callback=self.Detail_parse)

        '''       Pagination     '''
        page_no = response.meta['page_no']
        total_entries = response.meta['total_entries']
        if entry < total_entries:
            page_no += 1
            yield scrapy.Request(url=self.url.format(page_no), callback=self.parse_listing, headers=self.headers,
                                 meta={'page_no': page_no, 'entry': entry, 'total_entries': total_entries})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1182'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Altitude Real Estate'

            property_add = response.css('.col-address h5 ::text, .col-address h3 ::text').getall()  # full_adddress
            property_address = ', '.join(element.strip().replace('\n', ' ').replace('\t','') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.xpath("//*[contains(@title,'Bed')]/following-sibling::em[1]/text()").get('').strip()
            item['Field7'] = response.xpath("//*[contains(@title,'Bath')]/following-sibling::em[1]/text()").get('').strip()
            item['Field8'] = response.xpath("//*[contains(@title,'Car')]/following-sibling::em[1]/text()").get('').strip()

            # sold_price = response.xpath("//*[contains(text(),'Price')]/following-sibling::span[1]/text()").get('').strip().replace('Sold','').replace('for','')
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','').replace(' ','')   # sold_price

            description = response.css('.flex-grow p ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css("img.slide-image ::attr(src)").getall()
            new_img_urls = []
            for url in Agent_Imagess:
                new_img_urls.append(url)
            Images = ', '.join(new_img_urls)
            new_name = self.db.replace(' ', '').replace('/', '_')
            id = (response.url).split('/')[-2]
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            agent1 = response.css(".agent")[0]
            agent1_name = agent1.css('.agent-name a ::text').get('').strip()
            if agent1:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                agent1_phone = agent1.css(".agent-mobile ::text").get('').strip().replace("Call","")
                item['Field20'] = agent1_phone.strip()
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Images = agent1.css(".agent-wrapper-photo-image img ::attr(src)").get('').strip()
                new_name = agent1_name.replace(' ', '')
                id = agent1_phone.strip().replace(' ', '')
                # item['Field22'] = Agent_Images   # listing_images
                # print('Agent1 :',Agent_Images,',', new_name,',', id)
                images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            if len(response.css(".agent").getall()) > 1:
                agent2 = response.css(".agent")[1]
                agent2_name = agent2.css('.agent-name a ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    agent2_phone = agent2.css(".agent-mobile ::text").get('').strip().replace("Call", "")
                    item['Field26'] = agent2_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent2.css(".agent-wrapper-photo-image img ::attr(src)").get('').strip()
                    new_name = agent2_name.replace(' ', '')
                    id = agent2_phone.strip().replace(' ', '')
                    # item['Field27'] = Agent_Images
                    # print('Agent2 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field27'] = ", ".join(images)

            if len(response.css(".agent").getall()) == 3:
                agent3 = response.css(".agent")[2]
                agent3_name = agent3.css('.agent-name a ::text').get('').strip()
                if agent3:
                    item['Field28'] = agent3_name.strip()
                    agent3_phone = agent3.css(".agent-mobile ::text").get('').strip().replace("Call", "")
                    item['Field31'] = agent3_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent3.css(".agent-wrapper-photo-image img ::attr(src)").get('').strip()
                    new_name = agent3_name.replace(' ', '')
                    id = agent3_phone.strip().replace(' ', '')
                    # item['Field32'] = Agent_Images
                    # print('Agent3 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field32'] = ", ".join(images)

            item['Field33'] = response.xpath("//*[contains(text(),'Property ID')]/following-sibling::span[1]/text()").get('').strip()
            # item['Field35'] = response.xpath("//*[contains(text(),'Type')]/following-sibling::div[1]/text()").get('').strip()
            land_area = response.xpath("//*[contains(text(),'Land')]/following-sibling::span[1]/text()").get('').strip()
            if land_area:
                item['Field36'] = land_area.replace('m','').replace(' ','')   # land_area

            # feature_count = 58
            # for feature in response.css(".col-sm-4"):
            #     if 'Air Condition' in feature.css('::text').get('').strip():
            #         item['Field52'] = feature.css('::text').get('').strip()
            #     else:
            #         item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
            #         feature_count += 1

            print(item)
            self.insert_database(item)
        else:
            print('Data already exists')

    def download_image(self, img_url, file_dir, name):
        print('Download image details are : ',img_url, file_dir, name)
        try:
            response = requests.request(method='GET', url=img_url)
            if response.status_code == 200:
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                with open(os.path.join(file_dir, name), 'wb') as file:
                    file.write(response.content)
                print(f"{name} Image downloaded successfully.")
                return True
            else:
                print(f"Failed to download the image {img_url}. Status code:", response.status_code)
                return None
        except requests.exceptions.RequestException as e:
            print("An error occurred while downloading:", e)
        except Exception as e:
            print(e)

    def create_new_bucket(self, bucket_prefix, bucket_number, s3):
        new_bucket_name = f'{bucket_prefix}_{bucket_number}'
        s3.create_bucket(Bucket=new_bucket_name)
        return new_bucket_name

    def uploaded(self, list_of_img, names, id):
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
        bucket_prefix = self.bucket_prefix
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

            local_file_path = '/img'

            title_name = f'{names}_{id}_{index}.jpg'

            img_url = f'https://s3.ap-southeast-1.wasabisys.com/{current_bucket_name}/{title_name}'
            check_img = self.download_image(image_url, local_file_path, title_name)
            if check_img:
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

    def read_data_base(self, profileUrl):
        url = profileUrl
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]
        search_query = {"Field14": url}

        sale_urls_list_of_DB = []
        all_matching_data = collection.find_one(search_query)

        if all_matching_data:
            print(all_matching_data.get('Field5'))
            return True
        else:
            return False

    def insert_database(self, new_data):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]

        collection.insert_one(new_data)
        print("Data inserted successfully!")
