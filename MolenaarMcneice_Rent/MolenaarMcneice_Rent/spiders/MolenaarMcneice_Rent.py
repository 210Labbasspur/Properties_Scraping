#####################       MolenaarMcneice_Rent

import re
from copy import deepcopy
import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class MolenaarMcneice_Rent(scrapy.Spider):
    name = 'MolenaarMcneice_Rent'
    prefix = 'https://www.mmproperty.com.au'
    url = "https://www.mmproperty.com.au/wp-json/api/listings/all?priceRange=&type=rental&status=current&paged={}&limit=12&address=&bed=&bath=&car=&sort="
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        # 'cookie': 'afl_wc_utm_cookie_expiry=90; afl_wc_utm_sess_visit=1713651838; cfz_facebook-pixel=%7B%22Skxf_fb-pixel%22%3A%7B%22v%22%3A%22fb.3.1713651841935.280789014%22%2C%22e%22%3A1745187841935%7D%2C%22b66b_fb-pixel%22%3A%7B%22v%22%3A%22fb.3.1713651841935.41818852%22%2C%22e%22%3A1745187841935%7D%7D; afl_wc_utm_sess_landing=https%3A%2F%2Fwww.mmproperty.com.au%2Frecent-sales%2F; afl_wc_utm_utm_1st_url=https%3A%2F%2Fwww.mmproperty.com.au%2Frecent-sales%2F%3Futm_source%3Ddirect%26utm_medium%3Dnone; afl_wc_utm_utm_1st_visit=1713651838; afl_wc_utm_utm_url=https%3A%2F%2Fwww.mmproperty.com.au%2Frecent-sales%2F%3Futm_source%3Ddirect%26utm_medium%3Dnone; afl_wc_utm_utm_visit=1713651838; _gcl_au=1.1.127191139.1713651840; _gid=GA1.3.240486762.1713651844; afl_wc_utm_main={%22updated_ts%22:1713651842%2C%22cookie_expiry%22:90%2C%22consent_type%22:%22optout%22%2C%22sess_ts%22:1713651838%2C%22utm_1st_ts%22:1713651838%2C%22utm_ts%22:1713651838}; cf_chl_3=2619e88789d1135; cf_clearance=PJlPOm_t0M6NHglT7YgtM4GpVSMPS8adOzZnFcLxHXY-1713691138-1.0.1.1-I99hQVkVJzkEa.G3kPVA5Q4v8Mk9WwkDLh6o0.pAk2q60.TJSLCuFz3cAfMB02h6FyToTGEh1WoDlilY3TY9hQ; cf_chl_rc_m=10; _gat_gtag_UA_235416055_1=1; arp_scroll_position=521; _ga_F6G6GJCZFP=GS1.1.1713691136.2.1.1713691228.40.0.0; _ga=GA1.1.912485386.1713651839',
        'pagefrom': 'archive',
        'priority': 'u=1, i',
        # 'referer': 'https://www.mmproperty.com.au/recent-sales/',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"124.0.6367.60"',
        'sec-ch-ua-full-version-list': '"Chromium";v="124.0.6367.60", "Google Chrome";v="124.0.6367.60", "Not-A.Brand";v="99.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    count = 1
    db = 'MolenaarMcneice'
    collection = 'MolenaarMcneice_Rent'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        page_no = 1
        property_no = 0
        yield scrapy.Request(url=self.url.format(page_no), callback=self.parse, headers=self.headers,
                                meta = {'page_no': page_no, 'property_no': property_no})

    def parse(self, response):
        property_no = response.meta['property_no']
        data = json.loads(response.text)
        for property in data['results']:
            property_no += 1
            property_url = property.get('slug')
            print(self.count, property_url)
            self.count += 1

            mini_data = dict()
            mini_data['address'] = property.get('title')
            mini_data['bed'] = property.get('propertyBed')
            mini_data['bath'] = property.get('propertyBath')
            mini_data['car'] = property.get('propertyParking')
            mini_data['sold_price'] = property.get('propertyPricing').get('value').replace('Sold Price','').replace('SOLD','')
            mini_data['property_type'] = property.get('propertyCategory')
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse,
                                 meta={'mini_data':mini_data,'full_data':property})

        '''       Pagination     '''
        page_no = response.meta['page_no']
        total_results = data['total']
        if property_no < total_results:
            page_no += 1
            yield scrapy.Request(url=self.url.format(page_no), callback=self.parse, headers=self.headers,
                                 meta={'page_no': page_no, 'property_no': property_no})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            mini_data = response.meta['mini_data']
            full_data = response.meta['full_data']
            item = dict()
            item['Field2'] = '1229'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Molenaar Mcneice Property'

            item['Field5'] = mini_data.get('address')
            item['Field6'] = mini_data.get('bed')
            item['Field7'] = mini_data.get('bath')
            item['Field8'] = mini_data.get('car')
            # item['Field9'] = mini_data.get('sold_price').replace('$','').replace('-','').replace(' ','').replace('SOLD','')

            description = response.css('.listing-single__content-propertydesc ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            # Agent_Imagess = full_data['propertyImage']['listImg']
            new_img_urls = []
            for url in full_data['propertyImage']['listImg']:
                new_img_urls.append(url)
            Images = ', '.join(new_img_urls)
            new_name = self.db.replace(' ', '').replace('/', '_')
            id = full_data.get('uniqueID')
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            if response.css(".listing-single__agent-item"):
                agent1 = response.css(".listing-single__agent-item")[0]
                agent1_name = agent1.css('.text-white a ::text').get('').strip()
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field19'] =  agent1.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:','')
                    agent1_phone = agent1.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                    item['Field20'] = agent1_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images_style = agent1.css("::attr(style)").get('').strip()
                    Agent_Images = re.search(r"url\((.*?)\)", Agent_Images_style).group(1)
                    new_name = agent1_name.replace(' ', '')
                    id = agent1_phone.strip().replace(' ', '')
                    # item['Field22'] = Agent_Images   # listing_images
                    # print(f'Agent1 : {Agent_Images}, {new_name}, {id}')
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field22'] = ", ".join(images)

                if len(response.css(".listing-single__agent-item").getall()) > 1:
                    agent2 = response.css(".listing-single__agent-item")[1]
                    agent2_name = agent2.css('.text-white a ::text').get('').strip()
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        item['Field25'] = agent2.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                        agent2_phone = agent2.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:', '')
                        item['Field26'] = agent2_phone.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images_style = agent2.css("::attr(style)").get('').strip()
                        Agent_Images = re.search(r"url\((.*?)\)", Agent_Images_style).group(1)
                        new_name = agent2_name.replace(' ', '')
                        id = agent2_phone.strip().replace(' ', '')
                        # item['Field27'] = Agent_Images
                        # print(f'Agent2 : {Agent_Images}, {new_name}, {id}')
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field27'] = ", ".join(images)

                if len(response.css(".listing-single__agent-item").getall()) > 2:
                    agent3 = response.css(".listing-single__agent-item")[2]
                    agent3_name = agent3.css('.text-white a ::text').get('').strip()
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field30'] = agent3.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                        agent3_phone = agent3.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:', '')
                        item['Field31'] = agent3_phone.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images_style = agent3.css("::attr(style)").get('').strip()
                        Agent_Images = re.search(r"url\((.*?)\)", Agent_Images_style).group(1)
                        new_name = agent3_name.replace(' ', '')
                        id = agent3_phone.strip().replace(' ', '')
                        # item['Field32'] = Agent_Images
                        # print('Agent3 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field32'] = ", ".join(images)

            # item['Field33'] = mini_data.get('prop_id')
            item['Field35'] = mini_data.get('property_type')

            land_area = full_data['landDetails']['value']
            if land_area:
                item['Field36'] = land_area   # land_area

            feature_count = 58
            for feature in response.css(".listing-single__features-items"):
                if 'Air Condition' in feature.css('::text').get('').strip():
                    item['Field52'] = feature.css('::text').get('').strip()
                else:
                    item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
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
