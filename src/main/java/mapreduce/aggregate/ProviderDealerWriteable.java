package mapreduce.aggregate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.ComparisonChain;

public class ProviderDealerWriteable implements
		WritableComparable<ProviderDealerWriteable> {

	private long providerId;
	private long dealerId;

	public ProviderDealerWriteable() {}
	
	public ProviderDealerWriteable (long providerId, long dealerId) {
		this.providerId = providerId;
		this.dealerId = dealerId;
	}
	
	public void readFields(DataInput in) throws IOException {
		providerId = in.readLong();
		dealerId = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(providerId);
		out.writeLong(dealerId);
	}

	public int compareTo(ProviderDealerWriteable o) {
		return ComparisonChain.start().compare(providerId, o.providerId)
				.compare(dealerId, o.dealerId).result();
	}
	
	public int hashCode() {
	    final int prime = 31;
	    int result = 1;
	    result = (int) (prime * result + providerId);
	    result = (int) (prime * result + dealerId);
	    return result;
	}

	public String toString() {
		return "providerId = " + this.providerId + ", dealerId = " + this.dealerId;
	}
	
	@Override
	public boolean equals(Object other) {
	    if (!(other instanceof ProviderDealerWriteable)) {
	        return false;
	    }

	    ProviderDealerWriteable that = (ProviderDealerWriteable) other;

	    // Custom equality check here.
	    return this.providerId == that.providerId
	        && this.dealerId == that.dealerId;
	}

}