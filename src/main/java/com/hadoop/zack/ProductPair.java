package com.hadoop.zack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProductPair implements Writable, WritableComparable<ProductPair> {

    private Text productId;
    private Text neighborId;

    public ProductPair(){
        this.productId = new Text();
        this.neighborId = new Text();
    }

    public ProductPair(Text productId, Text neighborId){
        this.neighborId = neighborId;
        this.productId = productId;
    }

    public ProductPair(String productId, String neighborId){
        this(new Text(productId),new Text(neighborId));
    }

    public int compareTo(ProductPair other) {

        int value = this.productId.compareTo(other.getProductId());

        if(value != 0){
            return value;
        }


        if(this.neighborId.toString().equals("*")){

            if(other.getNeighborId().toString().equals("*")){

                return 0;

            }

            return -1;

        }else if (other.getNeighborId().toString().equals("*")){

            if(this.neighborId.toString().equals("*")){

                return 0;

            }

            return 1;

        }

        return this.neighborId.compareTo(other.getNeighborId());
    }

    public static ProductPair read(DataInput input) throws IOException {
        ProductPair pair = new ProductPair();
        pair.readFields(input);
        return pair;
    }

    public void write(DataOutput out) throws IOException {
        productId.write(out);
        neighborId.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        productId.readFields(in);
        neighborId.readFields(in);
    }

    @Override
    public String toString() {
        return "(" + getProductId() + ", " + getNeighborId() +")";
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) return true;
        if(obj == null || getClass() != obj.getClass()) return false;

        ProductPair pair = (ProductPair)obj;

        /*if(getProductId() != pair.getProductId()) return false;
        if(getNeighborId() != pair.getNeighborId()) return false;*/

        if (neighborId != null ? !neighborId.equals(pair.getNeighborId()) : pair.getNeighborId() != null) return false;
        if (productId != null ? !productId.equals(pair.getProductId()) : pair.getProductId() != null) return false;

        return true;

    }

    @Override
    public int hashCode() {
        /*int result = 17;

        result += 31 + productId.hashCode();
        result += 31 + neighborId.hashCode();

        return result;*/
        int result = productId != null ? productId.hashCode() : 0;
        result = 163 * result + (neighborId != null ? neighborId.hashCode() : 0);
        return result;
    }

    public Text getNeighborId() {
        return neighborId;
    }

    public Text getProductId() {
        return productId;
    }
}
