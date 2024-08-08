import os
import time
import uuid
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import requests
import tempfile
import pydicom
from PIL import Image
import io
import boto3
import uvicorn

app = FastAPI()

# S3 configuration
S3_BUCKET = "ci-files-v1"
S3_ACCESS_KEY = "AKIA23BPTFPQ7AVIKVUQ"
S3_SECRET_KEY = "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"
S3_REGION = "us-east-1"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION
)

class ImageRequest(BaseModel):
    urls: List[HttpUrl]

class ImageResponse(BaseModel):
    image: str
    metadata: dict

def download_image(url: str) -> bytes:
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to download image from {url}")
    return response.content

def extract_dicom_metadata(dicom_data: pydicom.dataset.FileDataset) -> dict:
    metadata = {}
    for elem in dicom_data.iterall():
        if elem.name != 'Pixel Data':
            metadata[elem.name] = str(elem.value)
    return metadata

def convert_dicom_to_jpeg(dicom_data: pydicom.dataset.FileDataset) -> bytes:
    pixel_array = dicom_data.pixel_array
    image = Image.fromarray(pixel_array)
    # Convert to RGB if necessary
    if image.mode != 'RGB':
        image = image.convert('RGB')
    img_byte_arr = io.BytesIO()
    image.save(img_byte_arr, format='JPEG')
    return img_byte_arr.getvalue()

def generate_unique_folder_name() -> str:
    unique_id = uuid.uuid4()
    timestamp = int(time.time())
    return f"{unique_id}_{timestamp}"

def generate_unique_filename() -> str:
    return str(uuid.uuid4())

def upload_to_s3(file_content: bytes, folder_name: str, file_name: str) -> str:
    s3_key = f"{folder_name}/{file_name}.jpg"
    
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_content)
        return f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading to S3: {str(e)}")

@app.post("/process_images/", response_model=List[ImageResponse])
async def process_images(request: ImageRequest):
    results = []
    folder_name = generate_unique_folder_name()
    
    for url in request.urls:
        try:
            # Download image
            image_content = download_image(url)
            
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.dcm') as temp_file:
                temp_file.write(image_content)
                temp_file_path = temp_file.name
            
            # Read DICOM file
            dicom_data = pydicom.dcmread(temp_file_path)
            
            # Extract metadata
            metadata = extract_dicom_metadata(dicom_data)
            
            # Convert to JPEG
            jpeg_content = convert_dicom_to_jpeg(dicom_data)
            
            # Generate unique filename
            unique_filename = generate_unique_filename()
            
            # Upload to S3
            s3_url = upload_to_s3(jpeg_content, folder_name, unique_filename)
            
            # Append result
            results.append(ImageResponse(image=s3_url, metadata=metadata))
            
            # Clean up temporary file
            os.unlink(temp_file_path)
        
        except HTTPException as he:
            # Re-raise HTTP exceptions
            raise he
        except Exception as e:
            # Catch all other exceptions
            raise HTTPException(status_code=500, detail=f"Error processing image {url}: {str(e)}")
    
    return results

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8007)