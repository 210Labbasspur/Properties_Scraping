#################       Rubinstein_Sold

import re
import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from scrapy import Selector
from selenium import webdriver

class Rubinstein_Sold(scrapy.Spider):
    name = 'Rubinstein_Sold'
    prefix = 'https://www.therubinsteingroup.com'
    url = 'https://www.therubinsteingroup.com/sold/'
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': '_ga=GA1.1.1019251377.1714476086; _ga_YT9J6BH05W=GS1.1.1714515337.3.0.1714515337.0.0.0; arp_scroll_position=498',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    count = 1
    db = 'Rubinstein'
    collection = 'Rubinstein_Sold'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        options = webdriver.ChromeOptions()
        options.add_experimental_option("detach", True)

        driver = webdriver.Chrome(options=options)
        driver.get('https://www.therubinsteingroup.com/sold/')
        time.sleep(2)

        '''         Scrolling down till end of the page             '''
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to the bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Wait for new content to load
            time.sleep(2)
            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        page_source = driver.page_source
        response = Selector(text=page_source)

        count = 1
        for property in response.xpath("//a[contains(@class,'card card--property')]"):
            api_url = "https://www.therubinsteingroup.com/page-data{}page-data.json"
            property_url = api_url.format(property.css('::attr(href)').get('').strip())
            print(count, property_url)
            count += 1
            yield scrapy.Request(url=property_url, callback=self.parse, headers=self.headers)

        time.sleep(5)
        driver.quit()

    def parse(self, response):
        data = json.loads(response.text)
        if data:
            property = data['result']['data']
            if property:
                property_url = self.prefix + data.get('path')
                self.count += 1
                mini_data = dict()
                mini_data['address'] = property['datoCmsProperty']['address']
                mini_data['bed'] = property.get('datoCmsProperty').get('beds')
                mini_data['bath'] = property.get('datoCmsProperty').get('baths')
                mini_data['car'] = property.get('datoCmsProperty').get('allCarSpaces')
                mini_data['property_url'] = property_url
                self.Detail_parse(mini_data, property)

    def Detail_parse(self, mini_data, full_data):
        property_url = mini_data['property_url']
        DB_already_exists = self.read_data_base(property_url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1042'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'The Rubinstein Group'

            item['Field5'] = mini_data.get('address')
            item['Field6'] = mini_data.get('bed')
            item['Field7'] = mini_data.get('bath')
            item['Field8'] = mini_data.get('car')

            sold_price = full_data.get('datoCmsProperty').get('price')
            if re.search(r'\d', sold_price):
                item['Field9'] = full_data.get('datoCmsProperty').get('price').replace('$','')

            item['Field12'] = ''
            if full_data.get('datoCmsProperty').get('description').get('value').get('document').get('children'):
                description = ''
                for desc in full_data.get('datoCmsProperty').get('description').get('value').get('document').get('children'):
                    if desc.get('children'):
                        description = description + desc.get('children')[0].get('value')
                item['Field12'] = description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            if full_data.get('datoCmsProperty').get('images')[0].get('all'):
                new_img_urls = []
                for url in full_data.get('datoCmsProperty').get('images')[0].get('all'):
                    new_img_urls.append(url.get('media')[0].get('image').get('url'))
                Images = ', '.join(new_img_urls)
                new_name = self.db.replace(' ', '').replace('/', '_')
                id = (property_url).split('/')[-2]
                # item['Field13'] = new_img_urls   # listing_images
                # print(Images,new_name,id)
                images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
                item['Field13'] = ", ".join(images)

            item['Field14'] = property_url
            '''          AGENTS          '''
            if full_data.get('datoCmsProperty').get('agents'):
                agent1 = full_data.get('datoCmsProperty').get('agents')[0]
                agent1_name = agent1.get('name')
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] = agent1.get('jobTitle')
                    item['Field19'] = agent1.get('email')
                    agent1_phone = agent1.get('phone')
                    item['Field20'] = agent1_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent1.get("profileImage").get('url')
                    new_name = agent1_name.replace(' ', '')
                    id = agent1_phone.strip().replace(' ', '')
                    # item['Field22'] = Agent_Images   # listing_images
                    # print('Agent1 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field22'] = ", ".join(images)

                if len(full_data.get('datoCmsProperty').get('agents')) > 1:
                    agent2 = full_data.get('datoCmsProperty').get('agents')[1]
                    agent2_name = agent2.get('name')
                    if agent2_name:
                        item['Field23'] = agent2_name.strip()
                        item['Field24'] = agent2.get('jobTitle')
                        item['Field25'] = agent2.get('email')
                        agent2_phone = agent2.get('phone')
                        item['Field26'] = agent2_phone.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent2.get("profileImage").get('url')
                        new_name = agent2_name.replace(' ', '')
                        id = agent2_phone.strip().replace(' ', '')
                        # item['Field27'] = Agent_Images
                        # print('Agent2 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field27'] = ", ".join(images)

                if len(full_data.get('datoCmsProperty').get('agents')) > 2:
                    agent3 = full_data.get('datoCmsProperty').get('agents')[2]
                    agent3_name = agent3.get('name')
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field29'] = agent3.get('jobTitle')
                        item['Field30'] = agent3.get('email')
                        agent3_phone = agent3.get('phone')
                        item['Field31'] = agent3_phone.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent3.get("profileImage").get('url')
                        new_name = agent3_name.replace(' ', '')
                        id = agent3_phone.strip().replace(' ', '')
                        # item['Field32'] = Agent_Images
                        # print('Agent3 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field32'] = ", ".join(images)

            # item['Field33'] = response.xpath("//*[contains(text(),'Property ID')]/text()").get('').strip().replace('Property ID:','')
            item['Field35'] = full_data.get('datoCmsProperty').get('propertyType')  # land_area
            item['Field36'] = full_data.get('datoCmsProperty').get('landArea')  # land_area

            if full_data.get('datoCmsProperty').get("features"):
                feature_count = 58
                for feature in full_data.get('datoCmsProperty').get("features"):
                    if 'Air Condition' in feature.get('feature'):
                        item['Field52'] = feature.get('feature')
                    else:
                        item[f'Field{feature_count}'] = feature.get('feature')
                        feature_count += 1

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

        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
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
