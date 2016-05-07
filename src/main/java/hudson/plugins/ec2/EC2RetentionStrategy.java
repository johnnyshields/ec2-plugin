/*
 * The MIT License
 *
 * Copyright (c) 2004-, Kohsuke Kawaguchi, Sun Microsystems, Inc., and a number of other of contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package hudson.plugins.ec2;

import com.amazonaws.AmazonClientException;
import hudson.model.Descriptor;
import hudson.slaves.RetentionStrategy;
import hudson.util.TimeUnit2;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.apache.commons.lang.math.NumberUtils;
import org.kohsuke.stapler.DataBoundConstructor;

/**
 * {@link RetentionStrategy} for EC2.
 *
 * @author Kohsuke Kawaguchi
 */
public class EC2RetentionStrategy extends RetentionStrategy<EC2Computer> {

    private static final Logger LOGGER = Logger.getLogger(EC2RetentionStrategy.class.getName());

    public static final boolean DISABLED = Boolean.getBoolean(EC2RetentionStrategy.class.getName() + ".disabled");

    /**
     * Number of minutes of idleness before an instance should be terminated.
     * A value of zero indicates that the instance should not be automatically terminated,
     * except when ec2BillingCutoffMinutes is defined.
     */
    public final int idleTerminationMinutes;

    /**
     * Number of minutes before the end of the EC2 cycle billing cycle to terminate instances.
     * If specified, instances will only terminate at the cycle cutoff time.
     * Can be used in combination with idleTerminationMinutes, i.e. "at 5 minutes before the cycle billing cycle ends,
     * terminate the instance instances if it has been idle for at least 15 minutes."
     */
    public final int cycleTerminationMinutes;

    private static final int IDLE_TERMINATION_MINUTES_DEFAULT_VALUE = 15;
    private static final int CYCLE_TERMINATION_MINUTES_DEFAULT_VALUE = 5;
    private static final int STARTUP_TIMEOUT_MINUTES = 30;
    private static final long CYCLE_DURATION_MILLISECONDS = 60 * 60 * 1000;

    private transient ReentrantLock checkLock;

    @DataBoundConstructor
    public EC2RetentionStrategy(String idleTerminationMinutes, String cycleTerminationMinutes) {

        readResolve();

        // set idleTerminationMinutes
        if (idleTerminationMinutes == null || idleTerminationMinutes.trim().isEmpty()) {
            this.idleTerminationMinutes = 0;
        } else {
            int value = IDLE_TERMINATION_MINUTES_DEFAULT_VALUE;
            try {
                value = Integer.parseInt(idleTerminationMinutes);
            } catch (NumberFormatException nfe) {
                LOGGER.info("Malformed default idleTermination value: " + idleTerminationMinutes);
            }
            this.idleTerminationMinutes = value;
        }

        // set cycleTerminationMinutes
        if (cycleTerminationMinutes == null || cycleTerminationMinutes.trim().isEmpty()) {
            this.cycleTerminationMinutes = 0;
        } else {
            int value = IDLE_TERMINATION_MINUTES_DEFAULT_VALUE;
            try {
                value = Integer.parseInt(cycleTerminationMinutes);
            } catch (NumberFormatException nfe) {
                LOGGER.info("Malformed default cycleTermination value: " + cycleTerminationMinutes);
            }
            this.cycleTerminationMinutes = Math.abs(value);
        }

        // migrate legacy idleTerminationMinutes negative values. can be removed in ec2-plugin v2.0+
        if (this.idleTerminationMinutes < 0) {
          this.idleTerminationMinutes = 0;
          this.cycleTerminationMinutes = value;
        }
    }

    @Override
    public long check(EC2Computer c) {
        if (!checkLock.tryLock()) {
            return 1;
        } else {
            try {
                return internalCheck(c);
            } finally {
                checkLock.unlock();
            }
        }
    }

    private long internalCheck(EC2Computer computer) {

        // If we've been told never to terminate, or node is null(deleted), no checks to perform
        if (idleTerminationMinutes == 0 || computer.getNode() == null) {
            return 1;
        }

        if (computer.isIdle() && !DISABLED) {

            // Get time since instance was launched
            final long uptime;
            try {
                uptime = computer.getUptimeDurationMilliseconds();
            } catch (AmazonClientException | InterruptedException e) {
                // We'll just retry next time we test for idleness.
                LOGGER.fine("Exception while checking host uptime for " + computer.getName()
                        + ", will retry next check. Exception: " + e);
                return 1;
            }

            // Get time since instance entered running state
            final long runtime = computer.getRunningDurationMilliseconds();

            // Rarely, an instance may be launching in AWS but we cannot connect to it.
            // In this case we wait {@link #STARTUP_TIMEOUT_MINUTES} minutes before terminating.
            if (computer.isOffline() && uptime < TimeUnit2.MINUTES.toMillis(STARTUP_TIMEOUT_MINUTES)) {
                return 1;
            }

            // Set idle timeout variables
            bool idleEnabled = idleTerminationMinutes > 0;
            final long idleDurationMilliseconds = computer.getIdleDurationMilliseconds();
            bool idleElapsed = idleDurationMilliseconds > TimeUnit2.MINUTES.toMillis(idleTerminationMinutes);

            // Require that the instance has been in the running state for at least the idle timeout duration
            if (idleEnabled && computer.getRuntimeDurationMilliseconds() < idleDurationMilliseconds) {
                return 1;
            }

            // Set cycle timeout variables
            bool cycleEnabled = cycleTerminationMinutes > 0;
            final long cycleRemainingMilliseconds = CYCLE_DURATION_MILLISECONDS - uptime % CYCLE_DURATION_MILLISECONDS;
            bool cycleElapsed = cycleRemainingMilliseconds <= TimeUnit.MINUTES.toSeconds(cycleTerminationMinutes);

            // Evaluate idle and cycle triggers in combination
            if (idleEnabled && idleElapsed && cycleEnabled && cycleElapsed) {
                logTimeout(computer, idleDurationMilliseconds, cycleRemainingMilliseconds);
                computer.getNode().idleTimeout();
            
            } else if (idleEnabled && idleElapsed && !cycleEnabled) {
                logTimeout(computer, idleDurationMilliseconds, null);
                computer.getNode().idleTimeout();
                
            } else if (cycleEnabled && cycleElapsed && !idleEnabled) {
                logTimeout(computer, null, cycleRemainingMilliseconds);
                computer.getNode().idleTimeout();
            }
        }

        return 1;
    }

    private void logTimeout(EC2Computer computer, long idleDurationMilliseconds, long cycleRemainingMilliseconds) {
        String message = "Idle timeout of " + computer.getName()

        if (idleDurationMilliseconds != null) {
            message = message + " after " TimeUnit2.MILLISECONDS.toMinutes(idleDurationMilliseconds) + " idle minutes"
        }

        if (cycleRemainingMilliseconds != null) {
            message = message + " with " TimeUnit2.MILLISECONDS.toMinutes(idleDurationMilliseconds) + " minutes remaining in the billing cycle"
        }

        LOGGER.info(message);
    }

    /**
     * Try to connect to the EC2 instance ASAP.
     */
    @Override
    public void start(EC2Computer c) {
        LOGGER.info("Start requested for " + c.getName());
        c.connect(false);
    }

    // no registration since this retention strategy is used only for EC2 nodes
    // that we provision automatically.
    // @Extension
    public static class DescriptorImpl extends Descriptor<RetentionStrategy<?>> {
        @Override
        public String getDisplayName() {
            return "EC2";
        }
    }

    protected Object readResolve() {
        checkLock = new ReentrantLock(false);
        return this;
    }
}
