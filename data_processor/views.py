from django.shortcuts import render
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .infer_data_types import infer_and_convert_data_types

# Create your views here.
@csrf_exempt
def upload_file(request):
    if request.method == 'POST' and request.FILES['file']:
        file = request.FILES['file']
        df = pd.read_csv(file)
        
        # Call the data type inference function
        processed_df = infer_and_convert_data_types(df)
        
        # Convert processed dataframe to JSON
        json_result = processed_df.to_json(orient='records')
        
        return JsonResponse({'data': json_result}, safe=False)
    
    return JsonResponse({'error': 'File not uploaded'}, status=400)