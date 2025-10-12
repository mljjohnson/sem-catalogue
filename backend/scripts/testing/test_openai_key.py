#!/usr/bin/env python3
"""
Test script to verify OpenAI API key is working
"""
import os
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(backend_dir))

from openai import OpenAI
from app.core.config import settings

def test_openai_key():
    """Test OpenAI API key"""
    print("🔑 Testing OpenAI API key...")
    print(f"   API Key: {settings.openai_api_key[:20]}..." if settings.openai_api_key else "   ❌ No API key found!")
    
    if not settings.openai_api_key:
        print("❌ OPENAI_API_KEY not set in environment")
        return False
    
    try:
        client = OpenAI(api_key=settings.openai_api_key)
        
        print("\n📝 Sending test completion request...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": "Say 'API key is working!' and nothing else."}
            ],
            max_tokens=20
        )
        
        result = response.choices[0].message.content
        print(f"✅ Response: {result}")
        print(f"   Model: {response.model}")
        print(f"   Tokens used: {response.usage.total_tokens}")
        
        print("\n✅ OpenAI API key is valid and working!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error testing OpenAI API: {e}")
        return False

if __name__ == "__main__":
    success = test_openai_key()
    sys.exit(0 if success else 1)

