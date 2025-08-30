##################      RandW_Sale

import re
from copy import deepcopy

import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class RandW_Sale(scrapy.Spider):
    name = 'RandW_Sale'
    prefix = 'https://www.randw.com.au'
    url = "https://www.randw.com.au/property-for-sale.html"
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '_ga_81YXVGDKWR=GS1.1.1714475052.1.0.1714475073.0.0.0; _ga_97HDK1EGYD=GS1.1.1714485132.2.0.1714485132.0.0.0; resolution=1366; _gid=GA1.3.1426666742.1714653104; _ga_VRS3WQEHGP=GS1.1.1714653104.3.1.1714653208.0.0.0; _ga=GA1.1.481249048.1714474731; arp_scroll_position=4501',
        'origin': 'https://www.randw.com.au',
        'priority': 'u=1, i',
        # 'referer': 'https://www.randw.com.au/property-sold.html?fgpload=1&IsOffmarket=-1',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }

    data = {
        'act': 'act_fgxml',
        '32[offset]': '0',
        '32[perpage]': '6',
        'SalesStageID[0]': 'LISTED_PT',
        'SalesStageID[1]': 'LISTED_LEASE',
        'SalesStageID[2]': 'LISTED_AUCTION',
        'SalesStageID[3]': 'DEPOSIT',
        'SalesStageID[4]': 'BOND',
        'SalesStageID[5]': 'EXCHANGED_UNREPORTED',
        'SalesCategoryID[0]': 'RESIDENTIAL_SALE',
        'SalesCategoryID[1]': 'RURAL_SALE',
        'SalesCategoryID[2]': 'COMMERCIAL_SALE',
        'SalesCategoryID[3]': 'LAND_SALE',
        'SalesCategoryID[4]': 'BUSINESS_SALE',
        'require': '0',
        'fgpid': '32',
        'ajax': '1',
    }

    count = 1
    db = 'RandW'
    collection = 'RandW_Sale'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        property_no = 0
        payload = deepcopy(self.data)
        payload['32[offset]'] = str(property_no)
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                             meta={'property_no': property_no})

    def parse(self, response):
        property_no = response.meta['property_no']
        for property in response.xpath("//*[contains(text(),'/residential-for-sale')]"):
            property_no += 1
            property_url = self.prefix + property.css('::text').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield response.follow(url=property_url, headers=self.headers, callback=self.Detail_parse)

        total_properties = response.css('totalrows ::text').get('').strip()
        print('Total Properties are : ', total_properties)
        if property_no < int(total_properties):
            payload = deepcopy(self.data)
            payload['32[offset]'] = str(property_no)
            yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                     meta={'property_no': property_no})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1036'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'R&W'

            property_add = response.css('.pd-address span ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\n', ' ').replace('\t','') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.xpath("//*[contains(@class,'pd-features')]/span[contains(text(),' Bed ')]/text()").get('').strip().replace('Bed','')
            item['Field7'] = response.xpath("//*[contains(@class,'pd-features')]/span[contains(text(),' Bath ')]/text()").get('').strip().replace('Bath','')
            item['Field8'] = response.xpath("//*[contains(@class,'pd-features')]/span[contains(text(),' Car ')]/text()").get('').strip().replace('Car','')

            # sold_price = response.css('.pd-price ::text').get('').strip().replace(' ','')
            # if re.search(r'\d', sold_price):
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.pd-description ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.xpath("//*[contains(@class,'box pd-slick-slide')]/div/img/@data-src").getall()
            new_img_urls = []
            for url in Agent_Imagess:
                new_img_urls.append(url)
            Images = ', '.join(new_img_urls)
            new_name = self.collection.replace(' ', '').replace('/', '_')
            id = self.count
            self.count += 1
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            if response.css(".pd-agentpanel--agent"):
                agent1 = response.css(".pd-agentpanel--agent")[0]
                agent1_name = agent1.css('.pd-agentpanel--agent-contactname ::text').get('').strip()
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] = agent1.css('.pd-agentpanel--agent-contactoffice ::text').get('').strip()
                    agent1_phone = agent1.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel://','')
                    item['Field20'] = agent1_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent1.css(".pd-agentpanel--agent-profile ::attr(src)").get('').strip()
                    new_name = agent1_name.replace(' ', '')
                    id = agent1_phone.strip().replace(' ', '')
                    # item['Field22'] = Agent_Images   # listing_images
                    # print('Agent1 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field22'] = ", ".join(images)

                if len(response.css(".pd-agentpanel--agent").getall()) > 1:
                    agent2 = response.css(".pd-agentpanel--agent")[1]
                    agent2_name = agent2.css('.pd-agentpanel--agent-contactname ::text').get('').strip()
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        item['Field24'] = agent2.css('.pd-agentpanel--agent-contactoffice ::text').get('').strip()
                        agent2_phone = agent2.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel://','')
                        item['Field26'] = agent2_phone.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent2.css(".pd-agentpanel--agent-profile ::attr(src)").get('').strip()
                        new_name = agent2_name.replace(' ', '')
                        id = agent2_phone.strip().replace(' ', '')
                        # item['Field27'] = Agent_Images
                        # print('Agent2 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field27'] = ", ".join(images)

                if len(response.css(".pd-agentpanel--agent").getall()) > 2:
                    agent3 = response.css(".pd-agentpanel--agent")[2]
                    agent3_name = agent3.css('.pd-agentpanel--agent-contactname ::text').get('').strip()
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field29'] = agent3.css('.pd-agentpanel--agent-contactoffice ::text').get('').strip()
                        agent3_phone = agent3.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel://','')
                        item['Field31'] = agent3_phone.strip()
                        agent3_mobile = agent3.css(".mobile a ::text").get('').strip()
                        item['Field31A'] = agent3_mobile.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent3.css(".pd-agentpanel--agent-profile ::attr(src)").get('').strip()
                        new_name = agent3_name.replace(' ', '')
                        id = agent3_phone.strip().replace(' ', '')
                        # item['Field32'] = Agent_Images
                        # print('Agent3 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field32'] = ", ".join(images)

            # item['Field33'] = response.xpath("//*[contains(text(),'Property ID')]/text()").get('').strip().replace('Property ID:','')
            # item['Field35'] = response.xpath("//*[contains(text(),'Type')]/following-sibling::div[1]/text()").get('').strip()
            land_area = response.xpath("//*[contains(text(),'Land area')]/text()").get('').strip()
            if land_area:
                item['Field36'] = land_area.replace('Land area','').replace('(approx)','').replace('sqm','').replace('-','').replace(' ','')   # land_area
            elif response.xpath("//*[contains(text(),'Building size')]/text()").get('').strip():
                item['Field36'] = land_area.replace('Building size','').replace('(approx)','').replace('sqm','').replace('-','').replace(' ','')   # land_area

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
