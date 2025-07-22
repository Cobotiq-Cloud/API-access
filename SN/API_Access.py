#!/usr/bin/env python3
"""
simple_robot_checker.py
--------------------------------------------------------------------
Simple script to check if robots can be accessed through the API.
Tests basic robot access without requiring OPEN_ACCESS_KEY.
--------------------------------------------------------------------
Requires: pip install requests
"""

import requests
import time

# ------------- CONFIG -------------------------------------------------

CLIENT_ID        = "tzOfAx4DomHbP8Qxj4Gyw2"
CLIENT_SECRET    = "33m8Ab2wj6GqGcQXQVO5qsPOKkA78H7NQaVbRidJIDeQfOIqhG4RCcVdwJw"
ACCESS_KEY_ID    = "f2a28756-6287-11f0-84ee-172017fd24fa"   
ACCESS_KEY_SECRET = "19638129da867bd1b709a52f4d8e13c5"    

SN_FILE          = "/Users/chenzaowen/Desktop/API-Access/API-access/SN/SN_unique.txt"            # file with serial numbers
API_ROOT         = "https://openapi.gs-robot.com"

# ------------- FUNCTIONS ----------------------------------------------

def get_token():
    """Get access token using the correct Gausium API format."""
    url = f"{API_ROOT}/gas/api/v1alpha1/oauth/token"
    
    # Use the correct grant type as shown in documentation
    payload = {
        "grant_type": "urn:gaussian:params:oauth:grant-type:open-access-token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "open_access_key": ACCESS_KEY_SECRET,  # Using ACCESS_KEY_SECRET as open_access_key
    }
    
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            token_data = r.json()
            print(f"✓ Token expires at: {token_data.get('expires_in', 'Unknown')}")
            return token_data["access_token"]
        else:
            print(f"❌ Token request failed with status {r.status_code}")
            print(f"Response: {r.text}")
    except Exception as e:
        print(f"❌ Error getting token: {e}")
    
    return None

def load_serial_numbers(filename):
    """Load serial numbers from file."""
    try:
        with open(filename, 'r') as f:
            sns = [line.strip() for line in f if line.strip()]
        return sns
    except:
        print(f"❌ Could not read {filename}")
        return []

def check_robot_access(token, sn):
    """Check if robot can be accessed via API."""
    headers = {"Authorization": f"Bearer {token}"}
    
    # Method 1: Check robots list
    try:
        url = f"{API_ROOT}/v1alpha1/robots"
        r = requests.get(url, params={"page": 1, "pageSize": 500}, headers=headers, timeout=10)
        
        if r.status_code == 200:
            robots = r.json().get("robots", [])
            for robot in robots:
                if robot["serialNumber"] == sn:
                    model = robot.get("modelTypeCode", "Unknown")
                    return True, f"Found in robots list - Model: {model}"
        elif r.status_code == 401:
            return False, "Authentication failed"
        elif r.status_code == 403:
            return False, "Access forbidden"
    except:
        pass
    
    # Method 2: Try direct task reports access
    try:
        url = f"{API_ROOT}/openapi/v2alpha1/robots/{sn}/taskReports"
        r = requests.get(url, params={"page": 1, "pageSize": 1}, headers=headers, timeout=10)
        
        if r.status_code == 200:
            return True, "Task reports accessible"
        elif r.status_code == 404:
            return False, "Robot not found"
        elif r.status_code == 401:
            return False, "Authentication failed"
        elif r.status_code == 403:
            return False, "Access forbidden"
    except:
        pass
    
    return False, "No access"

def main():
    print("🤖 Simple Robot API Access Checker")
    print("=" * 40)
    
    # Get token
    token = get_token()
    if not token:
        return
    print("✓ Got access token")
    
    # Load serial numbers
    serial_numbers = load_serial_numbers(SN_FILE)
    if not serial_numbers:
        return
    print(f"✓ Loaded {len(serial_numbers)} serial numbers")
    print()
    
    # Check each robot
    accessible = []
    not_accessible = []
    
    for i, sn in enumerate(serial_numbers, 1):
        has_access, reason = check_robot_access(token, sn)
        
        if has_access:
            accessible.append(sn)
            print(f"✓ {sn} - {reason}")
        else:
            not_accessible.append((sn, reason))
            print(f"❌ {sn} - {reason}")
        
        # Small delay to avoid rate limits
        time.sleep(0.1)
        
        # Progress every 20 robots
        if i % 20 == 0:
            print(f"\nProgress: {i}/{len(serial_numbers)}\n")
    
    # Summary
    print("\n" + "=" * 40)
    print("📊 SUMMARY")
    print("=" * 40)
    print(f"Total robots: {len(serial_numbers)}")
    print(f"✓ Accessible: {len(accessible)} ({len(accessible)/len(serial_numbers)*100:.1f}%)")
    print(f"❌ Not accessible: {len(not_accessible)} ({len(not_accessible)/len(serial_numbers)*100:.1f}%)")
    
    # Save simple results
    with open("accessible_robots.txt", "w") as f:
        f.write("ACCESSIBLE ROBOTS:\n")
        f.write("==================\n")
        for sn in accessible:
            f.write(f"{sn}\n")
    
    with open("not_accessible_robots.txt", "w") as f:
        f.write("NOT ACCESSIBLE ROBOTS:\n")
        f.write("======================\n")
        for sn, reason in not_accessible:
            f.write(f"{sn} - {reason}\n")
    
    print(f"\n💾 Results saved to:")
    print(f"  • accessible_robots.txt ({len(accessible)} robots)")
    print(f"  • not_accessible_robots.txt ({len(not_accessible)} robots)")

if __name__ == "__main__":
    main()