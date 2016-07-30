package org.apache.spark.prediction;

import java.io.Serializable;

public class TaskPrediction implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7438467146306275327L;
	private double sd;
	private double ave;
	public TaskPrediction(double sd,double ave)
	{
		this.sd=sd;
		this.ave=ave;
	}
	public int prediction()
	{

		double temp=Math.random()*2*(sd)+(ave-sd);
		temp=((int)(temp*100+0.5))/100.0;
		if(temp<0.01)temp=0.01;
		else if(temp>=1) return 1;
		return (int)(temp*100);
	}
	@Override
	public String toString() {
		return " [ sd=" + sd + ", ave=" + ave + "]";
	}
	
}
