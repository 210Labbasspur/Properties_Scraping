#####       RightChoice_Sold

import re

import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
class RightChoice_Sold(scrapy.Spider):
    name = 'RightChoice_Sold'
    prefix = 'https://www.rcre.com.au'
    # url = ("https://www.rcre.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=saleDate&query%5Bsold%5D%5Bvalue%5D=1&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Sale&query%5BsaleOrRental%5D%5Btype%5D="
    #        "equal&query%5Border%5D%5Bvalue%5D=saleDate-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1")
    url = ("https://www.rcre.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=saleDate&query%5Bsold%5D%5Bvalue%5D=1&query%5Bsold%5D%5Btype%5D=equal&query%5BsaleOrRental%5D%5Bvalue%5D=Sale&query%5BsaleOrRental%5D%5Btype%5D="
           "equal&query%5Border%5D%5Bvalue%5D=saleDate-desc&query%5Border%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={}")
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'content-length': '0',
        # 'cookie': '_gid=GA1.3.1679160112.1713519988; _gat=1; _gcl_au=1.1.1914071984.1713519988; __eventn_id=sviy1a9r9r; _ga=GA1.1.1747390558.1713519988; _ga_LJ3QB6FX5S=GS1.1.1713519989.1.1.1713520027.0.0.0; _ga_67P859NBLX=GS1.3.1713519989.1.1.1713520027.22.0.0',
        'origin': 'https://www.rcre.com.au',
        'priority': 'u=1, i',
        # 'referer': 'https://www.rcre.com.au/listings/?saleOrRental=Sale&sold=1&order=saleDate-desc',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    count = 1
    db = 'RightChoice'
    collection = 'RightChoice_Sold'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        page_no = 1
        yield scrapy.Request(url=self.url.format(page_no), callback=self.parse, headers=self.headers, meta={'page_no':page_no})

    def parse(self, response):
        data = json.loads(response.text)
        for property in data['data']['listings']:
            property_url = property.get('url')
            print(self.count, property_url)
            self.count += 1
            mini_data = dict()
            mini_data['address'] = property.get('displayAddress')
            mini_data['bed'] = property.get('detailsBeds')
            mini_data['bath'] = property.get('detailsBaths')
            mini_data['car'] = property.get('detailsCarAccom')
            mini_data['sold_price'] = property.get('price').replace('$','')

            mini_data['land_area'] = ''
            if property.get('landAreaUnit') == 'ac':
                land_area_sqm = float(property.get('landArea')) * 4046.85642
                mini_data['land_area'] = land_area_sqm
            elif property.get('landAreaUnit') == 'm2':
                mini_data['land_area'] = property.get('landArea')

            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse, meta={'mini_data':mini_data})

        '''     Pagination     '''
        total_pages = data['data']['max_num_pages']
        page_no = response.meta['page_no']
        if page_no < total_pages:
            page_no += 1
            yield scrapy.Request(url=self.url.format(page_no), callback=self.parse, headers=self.headers, meta={'page_no':page_no})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            mini_data = response.meta['mini_data']
            item['Field2'] = '1226'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Right Choice Real Estate'

            item['Field5'] = mini_data['address'].strip()  # full_adddress

            item['Field6'] = mini_data['bed'].strip()
            item['Field7'] = mini_data['bath'].strip()
            item['Field8'] = mini_data['car'].strip()

            item['Field9'] = mini_data['sold_price'].strip().replace('$','')

            description = response.css('.single-listing-description ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css("#media-gallery a.listing-media-slideimg ::attr(href)").getall()
            new_img_urls = []
            for url in Agent_Imagess:
                new_img_urls.append('https:' + url)
            Images = ', '.join(new_img_urls)
            new_name = self.db.replace(' ', '').replace('/', '_')
            id = (response.url).split('/')[-2]
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link

            '''          AGENTS          '''
            if response.css(".single-listing-agents .property-agent-details-wrapper .agent-mb"):
                agent1 = response.css(".single-listing-agents .property-agent-details-wrapper .agent-mb")[0]
                agent1_name = agent1.css('.staff-card-title a ::text').get('').strip()
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] = agent1.css('.staff-role ::text').get('').strip()
                    item['Field19'] =  agent1.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:','')
                    agent1_phone = agent1.xpath(".//*[contains(@data-phonelabel,'phone')]/a[1]/text()").get('').strip().replace('tel:','')
                    item['Field20'] = agent1_phone.strip()
                    agent1_mobile = agent1.xpath(".//*[contains(@data-phonelabel,'agent_mobile')]/a[1]/text()").get('').strip().replace('tel:','')
                    item['Field21'] = agent1_mobile.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images_style = agent1.css(".agent-mb-image-border ::attr(style)").get('').strip()
                    Agent_Images = re.search(r'url\((.*?)\)', Agent_Images_style).group(1)
                    new_name = agent1_name.replace(' ', '')
                    id = agent1_phone.strip().replace(' ', '')
                    # item['Field22'] = Agent_Images   # listing_images
                    # print(f'Agent1 : {Agent_Images}, {new_name}, {id}')
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field22'] = ", ".join(images)

                if len(response.css(".single-listing-agents .property-agent-details-wrapper .agent-mb").getall()) > 1:
                    agent2 = response.css(".single-listing-agents .property-agent-details-wrapper .agent-mb")[1]
                    agent2_name = agent2.css('.staff-card-title a ::text').get('').strip()
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        item['Field24'] = agent2.css('.staff-role ::text').get('').strip()
                        item['Field25'] = agent2.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                        agent2_phone = agent2.xpath(".//*[contains(@data-phonelabel,'phone')]/a[1]/text()").get('').strip().replace('tel:', '')
                        item['Field26'] = agent2_phone.strip()
                        agent2_mobile = agent2.xpath(".//*[contains(@data-phonelabel,'agent_mobile')]/a[1]/text()").get('').strip().replace('tel:', '')
                        item['Field26A'] = agent2_mobile.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images_style = agent2.css(".agent-mb-image-border ::attr(style)").get('').strip()
                        Agent_Images = re.search(r'url\((.*?)\)', Agent_Images_style).group(1)
                        new_name = agent2_name.replace(' ', '')
                        id = agent2_phone.strip().replace(' ', '')
                        # item['Field27'] = Agent_Images
                        # print(f'Agent2 : {Agent_Images}, {new_name}, {id}')
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field27'] = ", ".join(images)

                if len(response.css(".single-listing-agents .property-agent-details-wrapper .agent-mb").getall()) == 3:
                    agent3 = response.css(".single-listing-agents .property-agent-details-wrapper .agent-mb")[2]
                    agent3_name = agent3.css('.staff-card-title a ::text').get('').strip()
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field29'] = agent3.css('.staff-role ::text').get('').strip()
                        item['Field30'] = agent3.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                        agent3_phone = agent3.xpath(".//*[contains(@data-phonelabel,'phone')]/a[1]/text()").get('').strip().replace('tel:', '')
                        item['Field31'] = agent3_phone.strip()
                        agent3_mobile = agent3.xpath(".//*[contains(@data-phonelabel,'agent_mobile')]/a[1]/text()").get('').strip().replace('tel:', '')
                        item['Field31A'] = agent3_mobile.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images_style = agent3.css(".agent-mb-image-border ::attr(style)").get('').strip()
                        Agent_Images = re.search(r'url\((.*?)\)', Agent_Images_style).group(1)
                        new_name = agent3_name.replace(' ', '')
                        id = agent3_phone.strip().replace(' ', '')
                        # item['Field32'] = Agent_Images
                        # print('Agent3 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field32'] = ", ".join(images)

            prop_id = response.xpath("//*[contains(text(),'property ID')]/parent::div[1]/text()").getall()
            item['Field33'] = prop_id[-1].strip()  # description
            # item['Field35'] = response.xpath("//p[contains(text(),'Property Type')]/following-sibling::p[1]/text()").get('').strip()

            item['Field36'] = mini_data['land_area']

            feature_count = 58
            for feature in response.css(".b-features .section-body li.icon-included"):
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
