package org.apache.cassandra.auth.resource;

import org.apache.cassandra.auth.IResource;

public interface IResourceFactory {
   IResource fromName(String var1);
}
