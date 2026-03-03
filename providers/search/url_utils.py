import urllib.parse

def canonical_url(url: str) -> str:
    """Strip utm parameters, fragments, lowercase host."""
    try:
        parsed = urllib.parse.urlparse(url)
        # lowercase host
        netloc = parsed.netloc.lower()
        
        # strip utm params
        query_params = urllib.parse.parse_qsl(parsed.query)
        cleaned_params = [(k, v) for k, v in query_params if not k.lower().startswith('utm_')]
        query = urllib.parse.urlencode(cleaned_params)
        
        # remove fragment
        canonical = urllib.parse.urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, query, ''))
        return canonical
    except Exception:
        return url

def domain_quality(url: str) -> int:
    """Return quality score boost based on domain."""
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc.lower()
        
        if host.endswith('.gov.in') or host == 'rbi.org.in':
            return 20
        if 'reuters.com' in host or 'bloomberg.com' in host or 'mca.gov.in' in host:
            return 20
        if 'moneycontrol.com' in host or 'economictimes' in host:
            return 10
            
        return 0
    except Exception:
        return 0
