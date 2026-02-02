from flask import Flask, request, jsonify
from flask_cors import CORS
from awpy import Demo
import os
import time

app = Flask(__name__)
CORS(app)  # Allow frontend to connect

# Configure upload folder
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def home():
    return jsonify({"message": "CS:GO Demo Parser API"})

def safe_to_dicts(df):
    """Safely convert Polars DataFrame to list of dicts"""
    try:
        if df is None:
            return []
        return df.to_dicts()
    except Exception as e:
        print(f"Error converting to dicts: {e}")
        return []

@app.route('/parse', methods=['POST'])
def parse_demo():
    if 'demo' not in request.files:
        return jsonify({"error": "No demo file provided"}), 400
    
    file = request.files['demo']
    
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    # Save the uploaded file
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)
    
    try:
        print(f"Starting to parse: {filepath}")
        
        # Create Demo object and parse
        dem = Demo(filepath)
        dem.parse()
        
        print("Parse complete!")
        print(f"Demo object attributes: {dir(dem)}")
        
        # Build the response data
        parsed_data = {}
        
        # Try to get header info
        if hasattr(dem, 'header'):
            print(f"Header type: {type(dem.header)}")
            parsed_data['header'] = dem.header
        
        # Try to convert each DataFrame attribute
        # Note: Some DataFrames can be HUGE (grenades can have millions of rows)
        # We'll limit the size to avoid JSON serialization issues
        dataframe_attrs = {
            'rounds': None,      # Get all rounds
            'kills': None,       # Get all kills
            'damages': None,     # Get all damages
            'grenades': 1000,    # Limit to first 1000 grenades (can be millions!)
            'bomb': None,        # Get all bomb events
            'smokes': None,      # Get all smokes
            'infernos': None,    # Get all infernos
            'shots': 5000        # Limit to first 5000 shots
        }
        
        for attr, limit in dataframe_attrs.items():
            if hasattr(dem, attr):
                df = getattr(dem, attr)
                print(f"{attr} type: {type(df)}, is None: {df is None}")
                if df is not None:
                    try:
                        # Limit rows if specified
                        if limit and len(df) > limit:
                            df_limited = df.head(limit)
                            parsed_data[attr] = safe_to_dicts(df_limited)
                            print(f"{attr}: {len(parsed_data[attr])} rows (limited from {len(df)})")
                        else:
                            parsed_data[attr] = safe_to_dicts(df)
                            print(f"{attr}: {len(parsed_data[attr])} rows")
                    except Exception as e:
                        print(f"Error converting {attr}: {e}")
                        parsed_data[attr] = []
        
        print(f"Final data keys: {parsed_data.keys()}")
        
        # Important: Delete the demo object to release the file
        del dem
        
        # Wait a moment for Windows to release the file handle
        time.sleep(0.1)
        
        # Clean up the file after parsing
        try:
            os.remove(filepath)
            print(f"Cleaned up: {filepath}")
        except PermissionError:
            print(f"Warning: Could not delete {filepath} immediately")
        
        return jsonify({
            "success": True,
            "data": parsed_data
        })
    
    except Exception as e:
        print(f"Error parsing demo: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Clean up on error
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except PermissionError:
            print(f"Warning: Could not delete {filepath} after error")
        
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)