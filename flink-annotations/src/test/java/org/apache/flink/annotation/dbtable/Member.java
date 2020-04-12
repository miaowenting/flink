package org.apache.flink.annotation.dbtable;

/**
 * Description:
 * 类的注解 @DBTable 给定了值 MEMBER，将会用来作为表的名字
 *
 * @author mwt
 * @version 1.0
 * @date 2020-04-11
 */
@DBTable(name = "MEMBER")
public class Member {

	@SQLString(len = 30)
	String firstName;

	@SQLString(len = 50)
	String lastName;

	@SQLInteger
	Integer age;

	@SQLString(len = 30, constraints = @Constraints(primaryKey = true))
	String handle;

	static int memberCount;

	public String getFirstName() {
		return firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public Integer getAge() {
		return age;
	}

	public String getHandle() {
		return handle;
	}

	@Override
	public String toString() {
		return handle;
	}
}
