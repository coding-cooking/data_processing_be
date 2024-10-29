from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
from django.shortcuts import render
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .utils import infer_and_convert_data_types, analyze_column_types
from .models import ProcessedFile
import json
from django.core.serializers.json import DjangoJSONEncoder

# Create your views here.
logger = logging.getLogger(__name__)

class ProcessDataView(APIView):
    def post(self, request):
        if request.FILES['file']:
            file = request.FILES['file']

            # Read the file
            try:
                if file.name.endswith('.csv'):
                    preview = pd.read_csv(file, header=None, nrows=5)
                    print("Preview shape:", preview.shape)
                    print("First row:", preview.iloc[0].tolist())
                    
                    file.seek(0)
                    
                    if preview.iloc[0].notna().sum() == 1:
                        df = pd.read_csv(file, header=1)
                    else:
                        if preview.empty:
                            return Response({'error': 'File appears to be empty'}, status=status.HTTP_400_BAD_REQUEST)
                        
                        try:
                            df = pd.read_csv(file)
                        except:
                            file.seek(0)
                            df = pd.read_csv(file, header=None)
                            
                    print("Final DataFrame shape:", df.shape)
                            
                elif file.name.endswith(('.xls', '.xlsx')):
                    preview = pd.read_excel(file, header=None, nrows=5)
                    file.seek(0) 
                    
                    if preview.iloc[0].notna().sum() == 1:
                        df = pd.read_excel(file, header=1)
                    else:
                        if preview.empty:
                            return Response({'error': 'File appears to be empty'}, status=status.HTTP_400_BAD_REQUEST)
                        df = pd.read_excel(file)
                else:
                    return Response({'error': 'Unsupported file format'}, status=status.HTTP_400_BAD_REQUEST)
                    
            except Exception as e:
                logger.error(f"Error reading file: {str(e)}")
                return Response({'error': f'Error reading file: {str(e)}'}, status=status.HTTP_400_BAD_REQUEST)
            
            # Analyze and process the data
            original_analysis = analyze_column_types(df)
            processed_df = infer_and_convert_data_types(df)
            inferred_analysis = analyze_column_types(processed_df)
            
            # Prepare the response data
            response_data = {
                'original_analysis': original_analysis,
                'inferred_analysis': inferred_analysis,
                'data_sample': processed_df.head(10).to_dict(orient='records')
            }
            try:
                json_data = json.dumps(response_data, cls=DjangoJSONEncoder)
            except (TypeError, ValueError) as e:
                logger.error(f"Invalid JSON data: {str(e)}")
                return Response({'error': 'Data is not valid JSON'}, status=status.HTTP_400_BAD_REQUEST)
            
            try:
                processed_file = ProcessedFile.objects.create(
                    file=file,
                    processed_data=json_data  
                )
                json_data = json.dumps(response_data, cls=DjangoJSONEncoder)
                return Response({'processed_file_id': processed_file.id, 'data': json_data}, status=status.HTTP_200_OK)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {str(e)}")
                return Response({'error': 'Invalid JSON data'}, status=status.HTTP_400_BAD_REQUEST)
        
        return JsonResponse({'error': 'File not uploaded'}, status=400)