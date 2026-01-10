import os
try:
    from googleads import ad_manager, oauth2
    HAS_GOOGLEADS = True
except ImportError:
    HAS_GOOGLEADS = False

class GAMAPIHelper:
    """
    Helper to interact with Google Ad Manager API.
    Provides methods to generate VAST tags dynamically.
    """
    def __init__(self, json_key_path, network_code, application_name="Sariska-POC", mock=False):
        self.json_key_path = json_key_path
        self.network_code = network_code
        self.application_name = application_name
        self.mock = mock
        self.client = None

    def authenticate(self):
        """Authenticate using Service Account JSON key (or Mock)"""
        if self.mock:
            print("[GAM API MOCK] Authenticated via Mock Service Account")
            return True
        
        if not HAS_GOOGLEADS:
            print("[GAM API ERROR] googleads library not installed. Cannot use real API.")
            return False
            
        try:
            # Create a Service Account client
            oauth2_client = oauth2.GoogleServiceAccountClient(
                self.json_key_path, oauth2.GetAPIScope('ad_manager'))
            
            self.client = ad_manager.AdManagerClient(
                oauth2_client, self.application_name, self.network_code)
            return True
        except Exception as e:
            print(f"[GAM API ERROR] Authentication failed: {e}")
            return False

    def get_vast_tag_url(self, ad_unit_path):
        """
        Construct a VAST tag URL for a given ad unit.
        """
        if self.mock:
            print(f"[GAM API MOCK] Generating VAST tag for Ad Unit: {ad_unit_path}")
            # Return same sample but labeled as mock
            return f"https://pubads.g.doubleclick.net/gampad/ads?iu=/{self.network_code}/{ad_unit_path}&sz=640x480&cust_params=sample_ct%3Dlinear&ciu_szs=300x250%2C728x90&gdfp_req=1&output=vast&unviewed_position_start=1&env=vp&impl=s&correlator="
        
        base_url = "https://pubads.g.doubleclick.net/gampad/ads"
        params = {
            "iu": f"/{self.network_code}/{ad_unit_path}",
            "sz": "640x480",
            "gdfp_req": "1",
            "env": "vp",
            "output": "vast",
            "unviewed_position_start": "1",
            "correlator": "" # To be filled by the player
        }
        
        query_str = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query_str}"

    def list_ad_units(self):
        """Fetch list of ad units for verification"""
        if self.mock:
            return [
                {'name': 'Sample Ad Unit 1', 'path': 'external/single_ad_samples'},
                {'name': 'Sample Ad Unit 2', 'path': 'external/interactive_samples'}
            ]
            
        if not self.client: return []
        
        inventory_service = self.client.GetService('InventoryService', version='v202408')
        statement = ad_manager.StatementBuilder(version='v202408')
        
        ad_units = []
        response = inventory_service.getAdUnitsByStatement(statement.ToStatement())
        if 'results' in response and response['results']:
            for ad_unit in response['results']:
                ad_units.append({
                    'name': ad_unit['name'],
                    'path': ad_unit['adUnitCode']
                })
        return ad_units
