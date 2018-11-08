package org.apache.cassandra.service;

import org.apache.cassandra.exceptions.StartupException;
import org.slf4j.Logger;

public interface StartupCheck {
   void execute(Logger var1) throws StartupException;
}
