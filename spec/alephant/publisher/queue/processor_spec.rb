require "spec_helper"

describe Alephant::Publisher::Queue::Processor do
  before(:each) do
    allow_any_instance_of(
      Alephant::Publisher::Queue::Writer
    ).to receive(:initialize)

    allow_any_instance_of(
      Alephant::Publisher::Queue::Writer
    ).to receive(:run!)
  end

  describe "#consume" do
    it "Consume the message and deletes it" do
      message = instance_double(Aws::SQS::Message, :delete => nil)
      message_collection = [message]
      expect(message).to receive(:delete)
      subject.consume(message_collection)
    end
  end
end
