/* Each venmo payment transaction contains 
 * actor, target and the time stamp of the 
 * transaction happened. For each actor and
 * target, a unique number is assigned to identify
 * them uniquely */

public class Payment {
	// actor value
	int actor;
	// target value
	int target;
	// time stamp
	String time;

	// parameterized constructor to create a payment object
	Payment(int x, int y, String z) {
		actor = x;
		target = y;
		time = z;
	}

	// method to get value of an actor of a transaction
	public int getActor() {
		return actor;
	}

	// method to set value for an actor of a transaction

	public void setActor(int actor) {
		this.actor = actor;
	}

	// method to get value of a target of a transaction

	public int getTarget() {
		return target;
	}

	// method to set value for a target of a transaction

	public void setTarget(int target) {
		this.target = target;
	}

	// method to retrieve time stamp of a transaction
	public String getTime() {
		return time;
	}
	// method to set time stamp of a transaction
	public void setTime(String time) {
		this.time = time;
	}

}
