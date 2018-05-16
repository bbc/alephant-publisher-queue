require "spec_helper"

describe Alephant::Publisher::Queue do
  let(:options)     { Alephant::Publisher::Queue::Options.new }
  let(:fake_client) { Aws::SQS::Client.new(stub_responses: true) }

  before(:each) do
    allow_any_instance_of(Alephant::Publisher::Queue::Publisher).to receive(:sqs_client).and_return(fake_client)
    fake_client.stub_responses(:get_queue_url, { queue_url: 'http://sqs.aws.myqueue/id' })
  end

  describe ".create" do
    it "sets parser, sequencer, queue and writer" do
      options.add_queue(
        :sqs_queue_name => "bar",
        :aws_account_id => "foo"
      )
      instance = Alephant::Publisher::Queue.create(options)
      expect(instance.queue)
        .to be_a Alephant::Publisher::Queue::SQSHelper::Queue
    end

    context "with account" do
      it "creates a queue with an account number in the option hash" do
        options = Alephant::Publisher::Queue::Options.new
        options.add_queue(
          :sqs_queue_name => "bar",
          :aws_account_id => "foo"
        )

        publisher = Alephant::Publisher::Queue.create(options)

        expect(publisher.queue.queue).to be_a(Aws::SQS::Queue)
      end
    end

    context "without account" do
      it "creates a queue with an empty option hash" do
        options = Alephant::Publisher::Queue::Options.new
        options.add_queue(:sqs_queue_name => "bar")

        publisher = Alephant::Publisher::Queue.create(options)

        expect(publisher.queue.queue).to be_a(Aws::SQS::Queue)
      end
    end
  end
end
