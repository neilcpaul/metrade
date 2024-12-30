package org.ncp.metrade;

import org.ncp.core.service.Application;
import org.ncp.core.service.RunnableInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.ncp.core.util.thread.WaitUtils.exponentialBackoffWait;

@Application
public class METrade implements RunnableInstance {
    private static final Logger log = LoggerFactory.getLogger(METrade.class);

    @Override
    public void start() throws Exception {
        exponentialBackoffWait();
    }
}
