package org.ncp.metrade;

import org.ncp.core.Application;
import org.ncp.core.RunnableInstance;
import org.ncp.core.util.config.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.ncp.core.util.thread.WaitUtils.exponentialBackoffWait;

@Application
public class METrade implements RunnableInstance {
    private static final Logger log = LoggerFactory.getLogger(METrade.class);

    @Override
    public void start() throws Exception {
        exponentialBackoffWait();
    }
}
